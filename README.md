# Atlas Stream Processing 기반 Apache Iceberg Soft Delete 가이드

## 1. 문서 목적

이 문서는 MongoDB Atlas의 `sample_mflix.movies` 컬렉션을 Atlas Stream Processing(ASP)으로 처리하여 AWS S3의 Apache Iceberg 테이블로 적재하는 방법을 설명합니다.

핵심 요구사항은 다음과 같습니다.

- Atlas에서 생성되거나 변경된 영화 데이터는 Iceberg에 반영합니다.
- Atlas에서 문서가 삭제되어도 Iceberg 행을 물리적으로 삭제하지 않습니다.
- 삭제된 행은 `is_deleted=true`로 갱신합니다.
- 삭제 직전 문서의 영화 제목, 연도 등 기존 속성을 유지합니다.
- 기존 데이터는 최초 적재하고 이후 변경 사항은 실시간으로 반영합니다.

> 이 문서에서 `AWS Iceberg`는 AWS S3, AWS Glue Data Catalog, Apache Iceberg를 조합한 분석 저장소를 의미합니다.

## 2. 전체 구성

```text
MongoDB Atlas
sample_mflix.movies
        |
        | Change Stream
        v
Atlas Stream Processing
  1. 기존 데이터 Initial Sync
  2. Insert/Update/Replace 변환
  3. Delete 이벤트를 Soft Delete로 변환
        |
        v
AWS S3 Apache Iceberg
Glue Database: asp_analytics
Table: movies_current
        |
        v
Amazon Athena / Spark / BI
```

Iceberg 테이블에는 `_id`별 현재 상태를 한 행으로 유지합니다.

| Atlas 작업 | Iceberg 처리 | 최종 상태 |
| --- | --- | --- |
| `insert` | 행 생성 | `is_deleted=false` |
| `update` | 기존 행 갱신 | `is_deleted=false` |
| `replace` | 기존 행 교체 | `is_deleted=false` |
| `delete` | 삭제 직전 문서로 기존 행 갱신 | `is_deleted=true` |

## 3. 핵심 설계

### 3.1 일반 CDC만 사용하면 안 되는 이유

`$iceberg` 단계에서 `mode: "cdc"`를 사용하면 ASP는 `stream.source.operationType` 메타데이터를 보고 Iceberg 작업을 결정합니다.

원본 `delete` 이벤트를 그대로 전달하면 Iceberg에서도 행이 삭제됩니다. 문서에 단순히 `is_deleted=true`를 추가하는 것만으로는 충분하지 않습니다.

다음 두 처리가 모두 필요합니다.

1. 삭제 이벤트의 `fullDocumentBeforeChange`에 `is_deleted=true`를 추가합니다.
2. `$setStreamMeta`를 사용해 Iceberg에 전달되는 작업 유형을 `delete`에서 `update`로 변경합니다.

일반 필드인 `operationType`만 `$set`으로 바꾸면 안 됩니다. `$iceberg`는 일반 필드가 아닌 `stream.source.operationType` 메타데이터를 사용합니다.

### 3.2 삭제 전 문서가 필요한 이유

기본 Change Stream 삭제 이벤트에는 보통 다음 정보만 포함됩니다.

```javascript
{
  operationType: "delete",
  documentKey: {
    _id: ObjectId("...")
  }
}
```

이 정보만으로는 삭제된 영화의 `title`, `year`, `runtime` 등을 보존할 수 없습니다.

따라서 Atlas 컬렉션에서 Change Stream Pre/Post Image를 활성화하고 ASP의 `$source`에 다음 옵션을 사용합니다.

```javascript
config: {
  fullDocument: "required",
  fullDocumentBeforeChange: "required"
}
```

## 4. 예제 리소스 이름

| 구분 | 예제 값 |
| --- | --- |
| Atlas 데이터베이스 | `sample_mflix` |
| Atlas 컬렉션 | `movies` |
| ASP Atlas 연결 | `atlas_movies_src` |
| ASP S3 연결 | `aws_iceberg_s3` |
| ASP DLQ 연결 | `atlas_ops` |
| AWS 리전 | `ap-northeast-2` |
| S3 버킷 | `my-company-analytics-dev` |
| Glue Database | `asp_analytics` |
| Iceberg Table | `movies_current` |
| S3 경로 | `iceberg/asp_analytics/` |
| ASP 프로세서 | `moviesSoftDeleteIceberg` |

실제 환경에 맞게 연결 이름, 버킷, AWS 계정 ID와 리전을 변경해야 합니다.

컬렉션 이름이 `movies`가 아니라 `movie`라면 파이프라인의 `coll` 값만 변경합니다.

## 5. 사전 조건

- ASP가 연결할 수 있는 Atlas Dedicated Tier 클러스터가 필요합니다.
- Atlas 프로젝트에 `Project Stream Processing Owner` 권한이 필요합니다.
- 프로세서를 생성하는 데이터베이스 사용자는 `atlasAdmin` 또는 필요한 Stream Processing 권한이 있어야 합니다.
- Apache Iceberg 출력은 `SP10`, `SP30`, `SP50` 프로세서에서 지원됩니다.
- 기존 데이터를 함께 적재하려면 `initialSync`를 활성화해야 합니다.
- 삭제 전 문서를 가져오려면 Change Stream Pre/Post Image가 필요합니다.
- 소스 클러스터의 oplog window를 최소 24시간 이상으로 설정해야 합니다.
- 운영 환경의 oplog window는 예상 가능한 최대 ASP 중단 시간보다 길게 설정하는 것이 좋습니다.

## 6. Atlas 소스 설정

### 6.1 샘플 데이터 확인

Atlas Data Explorer 또는 소스 클러스터에 연결한 `mongosh`에서 확인합니다.

```javascript
db.getSiblingDB("sample_mflix").movies.findOne()
```

`sample_mflix`가 없다면 Atlas의 Load Sample Dataset 기능으로 샘플 데이터를 먼저 적재합니다.

### 6.2 Change Stream Pre/Post Image 활성화

다음 명령은 ASP Workspace가 아니라 소스 Atlas 클러스터에 연결해서 실행합니다.

```javascript
db.getSiblingDB("sample_mflix").runCommand({
  collMod: "movies",
  changeStreamPreAndPostImages: {
    enabled: true
  }
})
```

정상 결과 예시:

```javascript
{
  ok: 1
}
```

설정 확인:

```javascript
db.getSiblingDB("sample_mflix")
  .getCollectionInfos({ name: "movies" })[0].options
```

다음 설정이 보여야 합니다.

```javascript
{
  changeStreamPreAndPostImages: {
    enabled: true
  }
}
```

Pre/Post Image는 ASP 시작 및 삭제 테스트 전에 활성화해야 합니다. 활성화 이전에 이미 삭제된 데이터는 이 방식으로 복구할 수 없습니다.

### 6.3 Oplog Window 설정

Atlas 클러스터 설정에서 Minimum Oplog Window를 최소 24시간 이상으로 지정합니다.

ASP는 마지막 체크포인트의 Change Stream resume token으로 처리를 재개합니다. ASP 중단 시간이 oplog 보존 기간보다 길면 `ChangeStreamHistoryLost`가 발생할 수 있습니다.

운영 기준 예시:

| 예상 최대 복구 시간 | 권장 Oplog Window |
| --- | --- |
| 8시간 | 최소 24시간 |
| 24시간 | 48시간 이상 |
| 주말 장애 대응 | 72시간 이상 검토 |

## 7. AWS 준비

### 7.1 S3 버킷 생성

예제 버킷:

```text
my-company-analytics-dev
```

권장 설정:

- Block Public Access 활성화
- 기본 암호화 활성화
- ASP Workspace와 같은 AWS 리전 사용
- 개발, 검증, 운영 버킷 분리
- 수명 주기 정책 적용 전 Iceberg 파일 관리 방식 검토

Iceberg가 관리하는 경로의 파일에 일반적인 S3 Lifecycle 삭제 정책을 임의로 적용하면 테이블이 손상될 수 있습니다.

### 7.2 IAM Policy 생성

Atlas Unified AWS Access에서 사용할 IAM Role에 다음과 같은 정책을 부여합니다.

`111111111111`, 버킷 이름과 리전은 실제 값으로 변경합니다.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "IcebergS3Access",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:ListBucket",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:GetBucketLocation",
        "s3:AbortMultipartUpload",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::my-company-analytics-dev",
        "arn:aws:s3:::my-company-analytics-dev/*"
      ]
    },
    {
      "Sid": "IcebergGlueAccess",
      "Effect": "Allow",
      "Action": [
        "glue:CreateDatabase",
        "glue:GetDatabase",
        "glue:CreateTable",
        "glue:GetTable",
        "glue:UpdateTable"
      ],
      "Resource": [
        "arn:aws:glue:ap-northeast-2:111111111111:catalog",
        "arn:aws:glue:ap-northeast-2:111111111111:database/asp_analytics",
        "arn:aws:glue:ap-northeast-2:111111111111:table/asp_analytics/*"
      ]
    }
  ]
}
```

논리 삭제를 사용하더라도 `s3:DeleteObject` 권한이 필요합니다. 이는 분석 행을 삭제하기 위한 것이 아니라 Iceberg의 데이터 파일 재작성, 메타데이터 교체 및 오래된 파일 정리에 사용됩니다.

S3가 고객 관리 KMS Key로 암호화되어 있다면 다음 권한도 추가해야 합니다.

```text
kms:Encrypt
kms:Decrypt
kms:GenerateDataKey
kms:DescribeKey
```

### 7.3 AWS Trust 설정

IAM Trust Policy를 임의로 작성하지 않습니다. Atlas의 Unified AWS Access 설정 화면에서 제공하는 AWS Principal과 External ID를 사용해 IAM Role의 신뢰 관계를 구성합니다.

Athena 분석 사용자에게는 별도의 읽기 전용 IAM Role을 제공하는 것을 권장합니다. ASP 쓰기 Role을 분석 사용자와 공유하지 않습니다.

## 8. ASP Workspace 생성

Atlas UI에서 다음 순서로 생성합니다.

1. Atlas 프로젝트의 `Stream Processing` 메뉴로 이동합니다.
2. `Create a workspace`를 선택합니다.
3. Provider로 `AWS`를 선택합니다.
4. S3와 같은 리전인 `ap-northeast-2`를 선택합니다.
5. 기본 Tier와 최대 Tier를 `SP10` 이상으로 구성합니다.
6. Workspace 이름을 `analyticsWorkspace` 등으로 지정합니다.

S3와 다른 리전을 사용할 수도 있지만 네트워크 지연과 데이터 전송 비용을 고려해야 합니다.

## 9. Connection Registry 설정

Workspace의 `Manage` > `Connection Registry`에서 연결을 등록합니다.

### 9.1 Atlas 소스 연결

| 항목 | 값 |
| --- | --- |
| Connection Type | `Atlas Database` |
| Connection Name | `atlas_movies_src` |
| Cluster | `sample_mflix.movies`가 있는 클러스터 |
| 용도 | Change Stream Source |

소스 연결은 Change Stream과 초기 동기화를 읽을 수 있어야 합니다.

### 9.2 S3 연결

| 항목 | 값 |
| --- | --- |
| Connection Type | `S3` |
| Connection Name | `aws_iceberg_s3` |
| IAM Role | Unified AWS Access로 등록한 Role |
| 용도 | Iceberg Sink |

Iceberg 전용 Connection Type을 만드는 것이 아니라 S3 연결을 `$iceberg` 단계에서 사용합니다.

### 9.3 DLQ 연결

처리하지 못한 문서를 저장하기 위한 Atlas 연결을 준비합니다.

| 항목 | 값 |
| --- | --- |
| Connection Name | `atlas_ops` |
| Database | `asp_ops` |
| Collection | `movies_iceberg_dlq` |

DLQ 연결에는 해당 컬렉션에 대한 쓰기 권한이 필요합니다.

## 10. ASP 파이프라인

다음 파이프라인은 기존 데이터를 초기 동기화하고 이후 변경 사항을 Iceberg에 반영합니다.

```javascript
const pipeline = [
  {
    $source: {
      connectionName: "atlas_movies_src",
      db: "sample_mflix",
      coll: "movies",
      initialSync: {
        enable: true
      },
      config: {
        fullDocument: "required",
        fullDocumentBeforeChange: "required"
      }
    }
  },
  {
    $match: {
      operationType: {
        $in: ["insert", "update", "replace", "delete"]
      }
    }
  },
  {
    $set: {
      _aspIsDelete: {
        $eq: [
          { $meta: "stream.source.operationType" },
          "delete"
        ]
      },
      _aspSourceOperation: {
        $meta: "stream.source.operationType"
      },
      _aspEventAt: {
        $ifNull: [
          "$wallTime",
          { $meta: "stream.source.ts" }
        ]
      }
    }
  },
  {
    $replaceRoot: {
      newRoot: {
        $mergeObjects: [
          {
            $cond: {
              if: "$_aspIsDelete",
              then: "$fullDocumentBeforeChange",
              else: "$fullDocument"
            }
          },
          {
            is_deleted: "$_aspIsDelete",
            source_operation: "$_aspSourceOperation",
            source_event_at: "$_aspEventAt"
          },
          {
            $cond: {
              if: "$_aspIsDelete",
              then: {
                deleted_at: "$_aspEventAt"
              },
              else: {}
            }
          }
        ]
      }
    }
  },
  {
    $setStreamMeta: {
      "stream.source.operationType": {
        $cond: {
          if: {
            $eq: ["$source_operation", "delete"]
          },
          then: "update",
          else: "$source_operation"
        }
      }
    }
  },
  {
    $iceberg: {
      connectionName: "aws_iceberg_s3",
      bucket: "my-company-analytics-dev",
      databaseName: "asp_analytics",
      tableName: "movies_current",
      path: "iceberg/asp_analytics/",
      region: "ap-northeast-2",
      mode: "cdc",
      idFieldName: "_id",
      catalog: {
        type: "glue"
      }
    }
  }
]
```

### 10.1 `$source`

- `initialSync.enable=true`는 프로세서 최초 실행 시 기존 문서를 적재합니다.
- 초기 적재 완료 후 Change Stream 처리를 시작합니다.
- `fullDocument="required"`는 insert/update/replace 결과 문서를 제공합니다.
- `fullDocumentBeforeChange="required"`는 delete 직전 문서를 제공합니다.
- 필요한 이미지가 없으면 프로세서가 실패하므로 삭제 데이터가 조용히 유실되는 것을 방지합니다.

### 10.2 `$match`

Iceberg 현재 상태 테이블에 필요한 작업만 통과시킵니다.

`drop`, `rename`, `invalidate`와 같은 컬렉션 관리 이벤트는 처리하지 않습니다. 운영 환경에서는 이러한 이벤트가 발생했을 때 별도의 장애 대응 절차가 필요합니다.

### 10.3 `$set`

Change Stream 이벤트를 변환하기 전에 다음 정보를 임시 필드에 저장합니다.

| 필드 | 의미 |
| --- | --- |
| `_aspIsDelete` | 삭제 이벤트 여부 |
| `_aspSourceOperation` | 원본 작업 유형 |
| `_aspEventAt` | 원본 이벤트 또는 ASP 수신 시간 |

이 임시 필드는 `$replaceRoot` 이후 최종 Iceberg 행에는 남지 않습니다.

### 10.4 `$replaceRoot`

일반 이벤트는 `fullDocument`, 삭제 이벤트는 `fullDocumentBeforeChange`를 최종 행으로 사용합니다.

삭제 이벤트의 최종 문서 예시:

```javascript
{
  _id: ObjectId("..."),
  title: "Example Movie",
  year: 2026,
  runtime: 120,
  is_deleted: true,
  deleted_at: ISODate("2026-09-11T01:00:00Z"),
  source_operation: "delete",
  source_event_at: ISODate("2026-09-11T01:00:00Z")
}
```

### 10.5 `$setStreamMeta`

삭제 이벤트의 실제 데이터에는 `is_deleted=true`를 보존하면서 Iceberg가 수행할 작업은 `update`로 변경합니다.

```text
MongoDB operationType: delete
Iceberg operationType: update
```

이 단계가 없으면 `$iceberg`의 CDC 모드는 대상 행을 삭제합니다.

### 10.6 `$iceberg`

- `mode="cdc"`는 메타데이터의 작업 유형에 따라 insert/update/delete를 수행합니다.
- `idFieldName="_id"`는 MongoDB `_id`를 Iceberg 행 식별자로 사용합니다.
- `catalog.type="glue"`는 AWS Glue Data Catalog에 테이블을 등록합니다.
- 대상 테이블이 없으면 첫 메시지를 처리할 때 테이블이 생성됩니다.
- `$iceberg`는 파이프라인의 마지막 단계여야 합니다.

## 11. 프로세서 생성 및 시작

ASP Workspace의 `Connect` 버튼에서 연결 문자열을 확인하고 `mongosh`로 Workspace에 접속합니다.

DLQ와 SP10 Tier를 지정하여 프로세서를 생성합니다.

```javascript
sp.createStreamProcessor(
  "moviesSoftDeleteIceberg",
  pipeline,
  {
    tier: "SP10",
    dlq: {
      connectionName: "atlas_ops",
      db: "asp_ops",
      coll: "movies_iceberg_dlq"
    }
  }
)
```

프로세서 시작:

```javascript
sp.moviesSoftDeleteIceberg.start()
```

상태 확인:

```javascript
sp.moviesSoftDeleteIceberg.stats()
```

정상 상태에서는 프로세서가 실행 중이고 `dlqMessageCount`가 `0`이어야 합니다.

Atlas UI의 Stream Processing Monitoring 화면에서도 입력량, 출력량, DLQ 및 오류를 확인할 수 있습니다.

## 12. 기능 검증

기존 샘플 데이터를 삭제하지 말고 별도의 테스트 문서를 사용합니다.

### 12.1 테스트 문서 생성

소스 Atlas 클러스터에 연결하여 실행합니다.

```javascript
const testId = ObjectId()

db.getSiblingDB("sample_mflix").movies.insertOne({
  _id: testId,
  title: "ASP Soft Delete Test",
  year: 2026,
  runtime: 100,
  genres: ["Test"]
})

testId
```

출력된 24자리 ObjectId 문자열을 기록합니다.

### 12.2 업데이트 테스트

```javascript
db.getSiblingDB("sample_mflix").movies.updateOne(
  { _id: testId },
  {
    $set: {
      runtime: 120
    }
  }
)
```

Iceberg에서 `runtime=120`, `is_deleted=false`가 되어야 합니다.

### 12.3 삭제 테스트

```javascript
db.getSiblingDB("sample_mflix").movies.deleteOne({
  _id: testId
})
```

### 12.4 Athena 조회

ObjectId는 Iceberg에서 24자리 16진수 문자열로 변환됩니다.

```sql
SELECT
    _id,
    title,
    runtime,
    is_deleted,
    deleted_at,
    source_operation,
    source_event_at
FROM asp_analytics.movies_current
WHERE _id = '<testId의 24자리 문자열>';
```

기대 결과:

| 컬럼 | 기대 값 |
| --- | --- |
| `title` | `ASP Soft Delete Test` |
| `runtime` | `120` |
| `is_deleted` | `true` |
| `source_operation` | `delete` |
| 행 개수 | `1` |

행 개수 검증:

```sql
SELECT count(*) AS row_count
FROM asp_analytics.movies_current
WHERE _id = '<testId의 24자리 문자열>';
```

- 결과가 `0`이면 대상 행이 물리 삭제되었는지 확인합니다.
- 결과가 `2` 이상이면 CDC 키 처리 또는 중복 적재를 확인합니다.
- 결과가 `1`이고 `is_deleted=true`이면 Soft Delete가 정상입니다.

## 13. 분석용 View

일반 분석 사용자가 삭제 데이터를 실수로 포함하지 않도록 활성 데이터 View를 제공합니다.

```sql
CREATE OR REPLACE VIEW asp_analytics.movies_active AS
SELECT *
FROM asp_analytics.movies_current
WHERE is_deleted = false;
```

일반 BI와 분석 쿼리는 `movies_current` 대신 `movies_active`를 사용합니다.

삭제 데이터 감사 조회:

```sql
SELECT *
FROM asp_analytics.movies_current
WHERE is_deleted = true;
```

삭제 현황 집계:

```sql
SELECT
    is_deleted,
    count(*) AS movie_count
FROM asp_analytics.movies_current
GROUP BY is_deleted;
```

## 14. 데이터 타입 주의사항

ASP의 `$iceberg`는 BSON 값을 Iceberg 타입으로 변환합니다.

| MongoDB BSON | Iceberg 타입 또는 처리 |
| --- | --- |
| `ObjectId` | 16진수 문자열 |
| `string` | `string` |
| `int` | `int` |
| `long` | `long` |
| `double` | `double` |
| `bool` | `boolean` |
| `date` | `timestamptz` |
| `object` | Basic JSON 문자열 |
| `array` | Basic JSON 문자열 |

`sample_mflix.movies`에는 다음과 같은 배열과 객체 필드가 있습니다.

```text
genres
cast
directors
languages
countries
imdb
tomatoes
awards
```

이 필드들은 Athena에서 구조형 컬럼이 아닌 JSON 문자열로 보일 수 있습니다. 분석에서 자주 사용하는 속성은 ASP 파이프라인에서 별도 스칼라 컬럼으로 평탄화하는 방안을 검토합니다.

## 15. 운영 검증

### 15.1 수량 대사

Atlas 현재 문서 수:

```javascript
db.getSiblingDB("sample_mflix").movies.countDocuments({})
```

Iceberg 활성 문서 수:

```sql
SELECT count(*)
FROM asp_analytics.movies_current
WHERE is_deleted = false;
```

ASP 처리가 모두 완료된 시점에는 두 수량이 일치해야 합니다.

Iceberg 전체 건수는 삭제 이력이 남기 때문에 Atlas 현재 건수보다 클 수 있습니다.

### 15.2 필수 모니터링

- Stream Processor 상태가 `FAILED`인지 확인
- Input/Output message count 차이 확인
- DLQ message count 확인
- 처리 지연 확인
- Atlas oplog window 확인
- S3 저장량과 파일 개수 확인
- Glue 및 S3 권한 오류 확인
- Iceberg snapshot과 metadata 증가량 확인

### 15.3 DLQ 확인

```javascript
db.getSiblingDB("asp_ops")
  .movies_iceberg_dlq
  .find()
  .sort({ dlqTime: -1 })
```

지원하지 않는 BSON 타입, 스키마 충돌, IAM 권한 문제와 잘못된 동적 표현식 등이 DLQ 또는 프로세서 실패의 원인이 될 수 있습니다.

## 16. 장애 및 재처리 기준

ASP는 체크포인트에 Change Stream resume token을 저장합니다. 일반적인 재시작에서는 마지막 체크포인트부터 처리합니다.

다음 상황에서 주의가 필요합니다.

| 상황 | 권장 대응 |
| --- | --- |
| 일시적인 ASP 장애 | 기존 체크포인트로 재시작 |
| `ChangeStreamHistoryLost` | 새 테이블로 Initial Sync 재구축 |
| 파이프라인 의미 변경 | 새 프로세서와 새 테이블 생성 |
| 기존 테이블 스키마 충돌 | 새 테이블에서 검증 후 View 전환 |
| 물리 삭제 방식에서 전환 | 새 Soft Delete 테이블 재구축 |

`resumeFromCheckpoint=false`를 무분별하게 사용하면 누락 또는 중복이 발생할 수 있습니다.

안전한 운영 변경 방식은 다음과 같습니다.

```text
movies_current_v1 운영
        |
새 프로세서와 movies_current_v2 생성
        |
Initial Sync 및 수량 검증
        |
Athena View를 v2로 전환
        |
안정화 후 v1 정리
```

## 17. Iceberg 유지보수

Iceberg CDC는 업데이트할 때 기존 Parquet 파일을 직접 수정하지 않습니다. 새로운 데이터 파일, delete file 또는 snapshot metadata를 생성합니다.

따라서 다음 유지보수가 필요합니다.

- 작은 데이터 파일 병합
- 오래된 snapshot 만료
- 고아 파일 제거
- Athena 또는 Spark 기반 `OPTIMIZE` 정책
- 보존 정책에 맞는 `VACUUM` 정책

ASP와 Athena 또는 Spark가 같은 테이블에 동시에 쓰는 구조는 충분한 동시성 검증 없이 사용하지 않습니다. 가능하면 ASP를 해당 테이블의 단일 Writer로 운영합니다.

## 18. 주요 제약과 체크사항

- ASP는 at-least-once 처리 방식이므로 일부 이벤트가 재처리될 수 있습니다.
- Initial Sync 중에도 중복 이벤트가 발생할 수 있으므로 `_id`별 행 개수를 검증해야 합니다.
- Initial Sync 이전에 이미 삭제된 데이터는 복구할 수 없습니다.
- `fullDocumentBeforeChange="required"`인데 Pre Image가 없거나 만료되면 프로세서가 실패합니다.
- 삭제된 `_id`를 MongoDB에서 다시 사용하는 시나리오는 별도 테스트가 필요합니다.
- 가능하면 삭제된 `_id`를 재사용하지 않는 정책을 적용합니다.
- Iceberg 테이블을 이미 일반 CDC 물리 삭제 방식으로 운영했다면 과거 삭제 데이터는 별도 백업 없이는 복구할 수 없습니다.
- `drop`, `rename`, `invalidate` 이벤트에 대한 운영 대응 절차가 필요합니다.
- 실제 사용 리전의 ASP 버전에서 `$setStreamMeta`를 이용한 내장 `operationType` 변경이 정상 동작하는지 개발 환경에서 반드시 검증합니다.

## 19. 대안 설계

현재 ASP 환경에서 `$setStreamMeta`로 `stream.source.operationType`을 변경할 수 없다면 Append-Only 이력 테이블 패턴을 사용합니다.

이 경우 `$iceberg` 설정을 다음과 같이 변경합니다.

```javascript
{
  $iceberg: {
    connectionName: "aws_iceberg_s3",
    bucket: "my-company-analytics-dev",
    databaseName: "asp_analytics",
    tableName: "movies_history",
    path: "iceberg/asp_analytics/",
    region: "ap-northeast-2",
    mode: "insert",
    catalog: {
      type: "glue"
    }
  }
}
```

이 방식은 모든 변경 이벤트를 새 행으로 추가합니다. 원본 이력을 안전하게 보존할 수 있지만 `_id`별 현재 상태가 한 행으로 유지되지는 않습니다.

Athena View에서 `_id`별 최신 이벤트를 선택해야 합니다.

```sql
CREATE OR REPLACE VIEW asp_analytics.movies_current_v AS
SELECT *
FROM (
    SELECT
        h.*,
        row_number() OVER (
            PARTITION BY _id
            ORDER BY source_event_at DESC
        ) AS row_num
    FROM asp_analytics.movies_history h
)
WHERE row_num = 1;
```

동일한 이벤트 시간이 발생할 수 있는 운영 환경에서는 Change Stream resume token 등 추가 정렬 키를 보존하는 설계가 필요합니다.

## 20. 구축 완료 체크리스트

- [ ] `sample_mflix.movies` 컬렉션 확인
- [ ] Change Stream Pre/Post Image 활성화
- [ ] Oplog Window 24시간 이상 설정
- [ ] S3 버킷 생성 및 Public Access 차단
- [ ] Glue와 S3 IAM 권한 설정
- [ ] Unified AWS Access IAM Role 등록
- [ ] ASP Workspace를 SP10 이상으로 생성
- [ ] `atlas_movies_src` 연결 등록
- [ ] `aws_iceberg_s3` 연결 등록
- [ ] `atlas_ops` DLQ 연결 등록
- [ ] Soft Delete 파이프라인 생성
- [ ] Initial Sync 완료 확인
- [ ] Insert 테스트 성공
- [ ] Update 테스트 성공
- [ ] Delete 후 행 개수 `1` 확인
- [ ] Delete 후 `is_deleted=true` 확인
- [ ] Atlas와 Iceberg 활성 데이터 수량 대사
- [ ] Athena `movies_active` View 생성
- [ ] Processor 실패 및 DLQ Alert 구성
- [ ] Iceberg OPTIMIZE/VACUUM 정책 수립
- [ ] 장애 복구 및 재구축 절차 문서화

## 21. 공식 문서

- [MongoDB Atlas Stream Processing](https://www.mongodb.com/docs/atlas/atlas-stream-processing/)
- [Atlas Stream Processing `$source`](https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-source/)
- [Atlas Stream Processing `$iceberg`](https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-iceberg/)
- [Atlas Stream Processing `$setStreamMeta`](https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-setStreamMeta/)
- [Atlas Stream Processing Architecture](https://www.mongodb.com/docs/atlas/atlas-stream-processing/architecture/)
- [Atlas Stream Processing Limitations](https://www.mongodb.com/docs/atlas/atlas-stream-processing/limitations/)
- [AWS Athena Apache Iceberg](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
