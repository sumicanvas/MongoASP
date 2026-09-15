# Atlas Stream Processing to Apache Iceberg 최종 Runbook

## 1. 목적

이 문서는 MongoDB Atlas `sample_mflix.movies` 데이터를 Atlas Stream Processing(ASP)으로 처리하여 AWS S3의 Apache Iceberg 테이블에 저장하는 최종 설정과 검증 절차를 설명합니다.

검증은 두 단계로 분리합니다.

1. **Mock 테스트**: 실제 Atlas Change Stream을 사용하지 않고 합성 문서로 S3 연결과 Iceberg/Glue 생성을 검증합니다.
2. **전체 테스트**: 실제 `sample_mflix.movies` Change Stream으로 Initial Sync, Insert, Update, Delete, Soft Delete와 Checkpoint 복구를 검증합니다.


## 2. 최종 검증 결과

`sample_mflix_movies_v3`에서 다음 동작을 확인했습니다.

| 항목 | 결과 |
| --- | --- |
| S3 연결 | 성공 |
| Iceberg Metadata/Data 생성 | 성공 |
| Glue Catalog 테이블 생성 | 성공 |
| Initial Sync | 성공 |
| Live Insert | 성공 |
| Soft Delete | 성공 |
| Delete 후 기존 필드 보존 | 성공 |
| Delete 후 동일 `_id` 한 행 유지 | 성공 |
| Athena 조회 | 성공 |

검증에 사용한 문서:

```text
_id = 6aa78e3ba4f85561dc48a706
title = ASP_LIVE_DELETE_6aa78e3ba4f85561dc48a706
runtime = 888
```

Insert 후 상태:

```text
row_count = 1
isDeleted = false
operationType = insert
```

Delete 후 상태:

```text
row_count = 1
runtime = 888
isDeleted = true
operationType = delete
```

검증 증적:

- Insert: [`insert_cap.png`](./insert_cap.png)
- Soft Delete: [`afterdelete.png`](./afterdelete.png)
- 기존 `v2` 중복: [`listup-softdelete.png`](./listup-softdelete.png)

## 3. 최종 리소스 구성

| 항목 | 최종 값 |
| --- | --- |
| AWS Account ID | `979559056307` |
| AWS Region | `ap-northeast-2` |
| Atlas Source Connection | `atlas-con` |
| Atlas Database | `sample_mflix` |
| Atlas Collection | `movies` |
| Stream Processor | `sp01` |
| Processor Tier | `SP10` |
| ASP S3 Connection | `sumi-con-iceberg` |
| S3 Bucket | `sumi-asp-iceberg-bucket` |
| Iceberg Path | `sample-mflix-v3` |
| Glue Database | `mongodb_gluedb` |
| Iceberg Table | `sample_mflix_movies_v3` |

기존 `sample_mflix_movies_v2`에는 중복 `_id`가 확인됐으므로 최종 대상으로 사용하지 않습니다.

## 4. 전체 구조

```text
MongoDB Atlas
sample_mflix.movies
        |
        | Initial Sync + Change Stream
        v
Atlas Stream Processing
sp01 / SP10
        |
        | Unified AWS Access / STS AssumeRole
        v
Amazon S3
s3://sumi-asp-iceberg-bucket/sample-mflix-v3/...
        |
        +--- AWS Glue Data Catalog
        |    mongodb_gluedb.sample_mflix_movies_v3
        |
        v
Amazon Athena
```

## 5. AWS S3 설정

### 5.1 버킷 생성 기준
<img width="452" height="305" alt="image" src="https://github.com/user-attachments/assets/b6c8f4dd-4b08-47cc-a521-29bd50c19ba1" />

다음 설정을 사용합니다.

- 버킷 유형: General purpose
- 버킷 이름: `sumi-asp-iceberg-bucket`
- 리전: `ap-northeast-2`
- Block all public access: 활성화
- Object Ownership: Bucket owner enforced
- 기본 암호화: SSE-S3
- Object Lock: 테스트에서는 비활성화
- iceberg 로 연경할 경우 **버킷 이름에 점(`.`)을 사용하지 않음**

참고로, 점이 포함된 기존 버킷에서는 `$emit`이 성공해도 Iceberg Metadata 파일 생성이 실패했습니다.


### 5.2 S3 암호화

SSE-S3를 사용하면 별도의 KMS 권한이 필요하지 않습니다. 데모용이라 저는 SSE-S3 사용했으며 연결에 문제없었습니다.

SSE-KMS를 사용한다면 IAM Policy와 KMS Key Policy에 다음 권한이 추가로 필요합니다.

```text
kms:Encrypt
kms:Decrypt
kms:GenerateDataKey
kms:DescribeKey
```

## 6. AWS IAM 설정

Atlas Unified AWS Access에 등록한 IAM Role에 다음 Permission Policy를 부여합니다.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucketsForConnectionValidation",
      "Effect": "Allow",
      "Action": [
        "s3:ListAllMyBuckets"
      ],
      "Resource": "*"
    },
    {
      "Sid": "ReadIcebergBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::sumi-asp-iceberg-bucket"
    },
    {
      "Sid": "ManageIcebergObjects",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::sumi-asp-iceberg-bucket/*"
    },
    {
      "Sid": "ManageIcebergGlueCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:CreateDatabase",
        "glue:GetDatabase",
        "glue:CreateTable",
        "glue:GetTable",
        "glue:UpdateTable"
      ],
      "Resource": [
        "arn:aws:glue:ap-northeast-2:979559056307:catalog",
        "arn:aws:glue:ap-northeast-2:979559056307:database/mongodb_gluedb",
        "arn:aws:glue:ap-northeast-2:979559056307:table/mongodb_gluedb/*"
      ]
    }
  ]
}
```

확인 사항:

- Atlas가 사용하는 IAM Role에 이 정책이 연결되어야 합니다.
- Role 관련은 각사의 AWS IAM을 확인하셔야 합니다.

## 7. Atlas 연결 설정

### 7.1 Atlas 소스 연결

ASP Connection Registry에 Atlas Database 연결을 생성합니다.

```text
Connection Name: atlas-con
Database: sample_mflix
Collection: movies
Purpose: Initial Sync + Change Stream Source
```

### 7.2 S3 연결

Unified AWS Access에서 인증한 IAM Role을 사용해 S3 연결을 생성합니다.

```text
Connection Type: S3
Connection Name: sumi-con-iceberg
Region: ap-northeast-2
Purpose: Iceberg Sink
```

S3 연결에는 버킷 이름이 고정되지 않습니다. 실제 버킷과 Path는 `$iceberg` Stage에서 지정합니다.

## 8. MongoDB 소스 컬렉션 설정

Delete 직전 문서를 Iceberg에 보존하려면 Change Stream Pre/Post Image가 필요합니다. 이 명령은 ASP Workspace가 아니라 소스 Atlas 클러스터에서 실행합니다.

```javascript
db.getSiblingDB("sample_mflix").runCommand({
  collMod: "movies",
  changeStreamPreAndPostImages: {
    enabled: true
  }
})
```

확인:

```javascript
db.getSiblingDB("sample_mflix")
  .getCollectionInfos({ name: "movies" })[0]
  .options
  .changeStreamPreAndPostImages
```

기대 결과:

```javascript
{
  enabled: true
}
```

`fullDocumentBeforeChange: "required"`인데 Pre Image가 없거나 만료되면 이벤트가 실패하거나 DLQ로 이동할 수 있습니다.

소스 Atlas 클러스터의 Oplog Window는 예상 가능한 최대 Processor 중지 시간보다 길어야 하며 최소 24시간 이상을 권장합니다.

## 9. 최종 ASP Pipeline

다음 Pipeline은 MongoDB Delete를 Iceberg 물리 삭제가 아닌 Soft Delete Update로 변환합니다.

```javascript
var finalPipeline = [
  {
    $source: {
      connectionName: "atlas-con",
      db: "sample_mflix",
      coll: "movies",
      initialSync: {
        enable: true
      },
      config: {
        fullDocument: "updateLookup",
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
    $replaceRoot: {
      newRoot: {
        $cond: [
          {
            $eq: [
              { $meta: "stream.source.operationType" },
              "delete"
            ]
          },
          {
            $mergeObjects: [
              "$fullDocumentBeforeChange",
              "$documentKey",
              {
                isDeleted: true,
                deletedAt: "$wallTime",
                operationType: "delete"
              }
            ]
          },
          {
            $mergeObjects: [
              "$fullDocument",
              {
                isDeleted: false,
                operationType: {
                  $meta: "stream.source.operationType"
                }
              }
            ]
          }
        ]
      }
    }
  },
  {
    $setStreamMeta: {
      "stream.source": {
        $mergeObjects: [
          { $meta: "stream.source" },
          {
            operationType: {
              $cond: [
                "$isDeleted",
                "update",
                "$operationType"
              ]
            }
          }
        ]
      }
    }
  },
  {
    $iceberg: {
      connectionName: "sumi-con-iceberg",
      bucket: "sumi-asp-iceberg-bucket",
      region: "ap-northeast-2",
      path: "sample-mflix-v3",
      databaseName: "mongodb_gluedb",
      tableName: "sample_mflix_movies_v3",
      catalog: {
        type: "glue"
      },
      mode: "cdc",
      idFieldName: "_id"
    }
  }
]
```

### 9.1 Soft Delete 동작

MongoDB Delete 이벤트에는 다음 두 종류의 operation type이 존재합니다.

```text
데이터 필드 operationType = delete
stream.source.operationType = update
```

- 데이터 필드의 `operationType=delete`는 원본 작업을 분석 테이블에 보존합니다.
- Stream Metadata의 `operationType=update`는 `$iceberg mode=cdc`가 기존 행을 삭제하지 않고 갱신하도록 합니다.
- `fullDocumentBeforeChange`를 사용하므로 삭제 전의 `title`, `runtime` 같은 필드가 유지됩니다.

### 9.2 `deletedAt` 타입

활성 문서에는 `deletedAt: null`을 강제로 추가하지 않습니다. Initial Sync에서 모든 값이 `null`이면 Iceberg가 컬럼 타입을 추론하지 못할 수 있습니다.

최초 실제 Delete 이벤트의 `wallTime`으로 `deletedAt`을 생성합니다.

```javascript
deletedAt: "$wallTime"
```

`deletedAt`이 필수 요구사항이라면 전체 테스트 후 `DESCRIBE`에서 컬럼 생성과 타입을 별도로 확인해야 합니다.

## 10. Stream Processor 생성과 변경

### 10.1 새 Processor 생성

`sp01`이 존재하지 않을 때만 실행합니다.

```javascript
sp.createStreamProcessor(
  "sp01",
  finalPipeline,
  {
    tier: "SP10"
  }
)

sp.sp01.start()
```

### 10.2 기존 Processor를 v3로 최초 전환

기존 Processor 정보를 확인합니다.

```javascript
var processorDefinition = sp.listStreamProcessors()
  .find(item => item.name === "sp01")

printjson({
  name: processorDefinition.name,
  state: processorDefinition.state,
  tier: processorDefinition.tier
})
```

`sp.listStreamProcessors()`가 반환하는 `{ name, pipeline, tier }` 전체 객체를 `modify()`의 Pipeline으로 전달하면 안 됩니다. 기존 일부 Stage만 수정하지 말고 9절의 `finalPipeline` 전체를 사용해야 `deletedAt:null` 제거와 v3 대상 설정이 모두 적용됩니다.

Initial Sync를 새 v3 테이블에서 처음부터 실행할 때만 다음 명령을 사용합니다.

```javascript
sp.sp01.stop()

sp.sp01.modify(
  finalPipeline,
  { resumeFromCheckpoint: false }
)

sp.sp01.start()
```

`resumeFromCheckpoint=false`는 Initial Sync를 처음부터 재실행합니다. 같은 Iceberg 테이블에 반복 실행하면 중복이 발생할 수 있으므로 v3 최초 구축 이후에는 반복하지 않습니다.

### 10.3 일반 Pipeline 변경

Initial Sync가 완료된 Processor의 일반 Pipeline 변경에서는 Checkpoint를 유지합니다.

```javascript
sp.sp01.stop()

sp.sp01.modify(
  finalPipeline,
  { resumeFromCheckpoint: true }
)

sp.sp01.start()
```

소스 의미 또는 대상 테이블을 크게 변경한다면 기존 테이블을 덮어쓰지 말고 새 버전의 Path/Table을 생성하는 방식을 권장합니다.

## 11. 실행 상태 확인

### 11.1 Processor 상태

현재 ASP 응답에서는 상태가 `stats.status`가 아니라 최상위 `state`에 있습니다.

```javascript
var current = sp.sp01.stats({
  options: { verbose: true }
})

printjson({
  state: current.state,
  errorMsg: current.errorMsg,
  input: current.stats.inputMessageCount,
  output: current.stats.outputMessageCount,
  dlq: current.stats.dlqMessageCount,
  lastCheckpoint: current.stats.lastCheckpoint
})
```

정상 기준:

```text
state = STARTED
errorMsg = 빈 문자열
lastCheckpoint = 최근 시각
```

### 11.2 Initial Sync

```javascript
printjson(
  current.stats.operatorStats[0].targetStats
)
```

정상 완료 예시:

```javascript
{
  initialSync: {
    estimatedDocs: Long("21356"),
    copiedDocs: Long("21356"),
    status: "completed"
  }
}
```

Initial Sync가 `in_progress`이면 Processor를 재시작하지 않습니다. ASP는 기존 문서 복사 후 새로운 Change Event를 재생합니다.

### 11.3 DLQ

현재 Processor에는 과거 테스트에서 누적된 DLQ가 있으므로 전체 누적값이 `0`인지가 아니라 각 테스트 전후 Delta를 확인합니다.

```javascript
var beforeStats = sp.sp01.stats().stats

printjson({
  output: beforeStats.outputMessageCount,
  dlq: beforeStats.dlqMessageCount
})
```

테스트 후 `dlqMessageCount`가 증가하지 않아야 합니다.

Processor의 DLQ 설정 확인:

```javascript
var spDefinition = sp.listStreamProcessors()
  .find(item => item.name === "sp01")

printjson(spDefinition.dlq)
```

실시간 오류 샘플링:

```javascript
sp.sp01.sample()
```

실패 문서가 발생하면 `_dlqMessage.errInfo.reason`과 `_dlqMessage.doc`를 확인합니다. 확인 후 `Ctrl+C`로 종료합니다.

## 12. Mock 테스트

Mock 테스트는 실제 Atlas Change Stream과 `sample_mflix.movies`를 변경하지 않습니다.

### 12.1 Mock 테스트 범위

| 검증 항목 | 포함 여부 |
| --- | --- |
| Atlas에서 AWS IAM Role Assume | 포함 |
| S3 읽기/쓰기 권한 | 포함 |
| Glue Catalog 권한 | Iceberg Mock에 포함 |
| Iceberg Metadata/Data 생성 | Iceberg Mock에 포함 |
| Atlas Change Stream | 미포함 |
| Pre/Post Image | 미포함 |
| CDC Update/Delete | 미포함 |
| Soft Delete | 미포함 |

### 12.2 Mock A: S3 `$emit` 테스트

ASP Workspace에서 실행합니다.

```javascript
sp.process([
  {
    $source: {
      documents: [
        {
          _id: "asp-s3-mock-1",
          message: "S3 connectivity mock",
          createdAt: new Date()
        }
      ]
    }
  },
  {
    $emit: {
      connectionName: "sumi-con-iceberg",
      bucket: "sumi-asp-iceberg-bucket",
      region: "ap-northeast-2",
      path: "asp-s3-mock",
      config: {
        outputFormat: "relaxedJson",
        writeOptions: {
          count: 1
        }
      }
    }
  }
])
```

S3에서 확인합니다.

```text
s3://sumi-asp-iceberg-bucket/asp-s3-mock/
```

성공 조건:

- `sp.process()`가 오류 없이 완료됩니다.
- S3 Prefix에 JSON 파일이 생성됩니다.
- `s3:ListBucket`, `s3:GetBucketLocation`, `s3:PutObject`가 정상입니다.

이 테스트가 성공해도 Glue와 Iceberg는 아직 검증되지 않은 상태입니다.

### 12.3 Mock B: 최소 Iceberg 테스트

```javascript
sp.process([
  {
    $source: {
      documents: [
        {
          _id: "asp-iceberg-mock-1",
          title: "ASP Iceberg Mock",
          createdAt: new Date()
        }
      ]
    }
  },
  {
    $iceberg: {
      connectionName: "sumi-con-iceberg",
      bucket: "sumi-asp-iceberg-bucket",
      region: "ap-northeast-2",
      path: "asp-iceberg-mock-v1",
      databaseName: "mongodb_gluedb",
      tableName: "asp_iceberg_mock_v1",
      catalog: {
        type: "glue"
      },
      mode: "insert"
    }
  }
], {
  tier: "SP10"
})
```

S3 확인 경로:

```text
s3://sumi-asp-iceberg-bucket/
  asp-iceberg-mock-v1/
    mongodb_gluedb/
      asp_iceberg_mock_v1/
        metadata/
        data/
```

Glue 확인:

```text
Database: mongodb_gluedb
Table: asp_iceberg_mock_v1
```

Athena 확인:

```sql
SELECT *
FROM "mongodb_gluedb"."asp_iceberg_mock_v1"
WHERE "_id" = 'asp-iceberg-mock-1';
```

성공 조건:

- S3에 Iceberg Metadata와 Data가 생성됩니다.
- Glue 테이블이 생성됩니다.
- Athena에서 Mock 문서가 한 건 조회됩니다.

Mock 테스트를 반복하면 `mode=insert` 특성상 중복 행이 생길 수 있습니다. 반복할 때는 새 `_id` 또는 새 Mock Table을 사용합니다.

## 13. 전체 End-to-End 테스트

전체 테스트는 Processor가 `STARTED`이고 Initial Sync가 `completed`인 상태에서 수행합니다.

### 13.1 사전 확인

ASP Workspace:

```javascript
var beforeE2E = sp.sp01.stats({
  options: { verbose: true }
})

printjson({
  state: beforeE2E.state,
  errorMsg: beforeE2E.errorMsg,
  output: beforeE2E.stats.outputMessageCount,
  dlq: beforeE2E.stats.dlqMessageCount
})

printjson(beforeE2E.stats.operatorStats[0].targetStats)
```

확인 사항:

- `state=STARTED`
- `errorMsg`가 빈 문자열
- `initialSync.status=completed`
- 테스트 시작 전 Output과 DLQ 값 기록

### 13.2 테스트 문서 Insert

소스 Atlas 클러스터:

```javascript
var sourceDb = db.getSiblingDB("sample_mflix")
var e2eId = ObjectId()
var e2eIdString = e2eId.toHexString()
var e2eTitle = "ASP_E2E_" + e2eIdString

sourceDb.movies.insertOne({
  _id: e2eId,
  title: e2eTitle,
  year: NumberInt(2099),
  runtime: NumberInt(100),
  type: "movie"
})

printjson({
  id: e2eIdString,
  title: e2eTitle,
  idLength: e2eIdString.length
})
```

`idLength`는 반드시 `24`여야 합니다.

Athena에서 Insert가 확인될 때까지 조회합니다.

```sql
SELECT
    "_id",
    title,
    runtime,
    "isDeleted",
    "operationType"
FROM "mongodb_gluedb"."sample_mflix_movies_v3"
WHERE "_id" = '<E2E_ID>';
```

Insert 성공 조건:

```text
row_count = 1
runtime = 100
isDeleted = false
operationType = insert
```

Insert가 보이기 전에 Update 또는 Delete를 실행하지 않습니다.

### 13.3 Update

소스 Atlas 클러스터:

```javascript
sourceDb.movies.updateOne(
  { _id: e2eId },
  {
    $set: {
      runtime: NumberInt(120),
      plot: "updated by ASP E2E test"
    }
  }
)
```

Athena:

```sql
SELECT
    "_id",
    title,
    runtime,
    "isDeleted",
    "operationType"
FROM "mongodb_gluedb"."sample_mflix_movies_v3"
WHERE "_id" = '<E2E_ID>';
```

Update 성공 조건:

```text
row_count = 1
runtime = 120
isDeleted = false
operationType = update
```

이 결과가 확인된 후 Delete를 실행합니다.

### 13.4 Delete와 Soft Delete

소스 Atlas 클러스터:

```javascript
var deleteResult = sourceDb.movies.deleteOne({
  _id: e2eId
})

if (deleteResult.deletedCount !== 1) {
  throw new Error("Delete 대상 문서가 정확히 한 건이 아닙니다.")
}

var sourceAfterDelete = sourceDb.movies.findOne({
  _id: e2eId
})

printjson({
  deletedCount: deleteResult.deletedCount,
  sourceAfterDelete: sourceAfterDelete
})
```

MongoDB 성공 조건:

```text
deletedCount = 1
sourceAfterDelete = null
```

Athena:

```sql
SELECT
    "_id",
    title,
    runtime,
    "isDeleted",
    "operationType"
FROM "mongodb_gluedb"."sample_mflix_movies_v3"
WHERE "_id" = '<E2E_ID>';
```

Soft Delete 성공 조건:

```text
row_count = 1
runtime = 120
isDeleted = true
operationType = delete
```

이 결과가 의미하는 내용:

- MongoDB에서는 문서가 물리 삭제됐습니다.
- Iceberg 행은 물리 삭제되지 않았습니다.
- Delete 직전의 `runtime=120`이 보존됐습니다.
- 원본 작업 유형 `delete`가 보존됐습니다.
- Iceberg CDC 작업은 내부적으로 Update로 처리됐습니다.

### 13.5 자동 PASS/FAIL 판정

```sql
WITH target AS (
    SELECT *
    FROM "mongodb_gluedb"."sample_mflix_movies_v3"
    WHERE "_id" = '<E2E_ID>'
)
SELECT
    count(*) AS row_count,
    sum(CASE WHEN runtime = 120 THEN 1 ELSE 0 END)
        AS preserved_runtime_count,
    sum(CASE WHEN "isDeleted" = true THEN 1 ELSE 0 END)
        AS soft_deleted_count,
    sum(CASE WHEN "operationType" = 'delete' THEN 1 ELSE 0 END)
        AS delete_operation_count,
    CASE
        WHEN count(*) = 1
         AND sum(CASE WHEN runtime = 120 THEN 1 ELSE 0 END) = 1
         AND sum(CASE WHEN "isDeleted" = true THEN 1 ELSE 0 END) = 1
         AND sum(CASE WHEN "operationType" = 'delete' THEN 1 ELSE 0 END) = 1
        THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result
FROM target;
```

정상 결과:

```text
row_count = 1
preserved_runtime_count = 1
soft_deleted_count = 1
delete_operation_count = 1
test_result = PASS
```

### 13.6 Processor와 DLQ Delta 확인

```javascript
var afterE2E = sp.sp01.stats({
  options: { verbose: true }
})

printjson({
  state: afterE2E.state,
  errorMsg: afterE2E.errorMsg,
  output: afterE2E.stats.outputMessageCount,
  dlq: afterE2E.stats.dlqMessageCount
})
```

성공 조건:

- `state=STARTED`
- `errorMsg`가 빈 문자열
- Output이 테스트 전보다 증가
- DLQ가 테스트 전보다 증가하지 않음
- 최근 Checkpoint 시각이 갱신됨

### 13.7 중복 확인

테스트 ID:

```sql
SELECT
    "_id",
    count(*) AS row_count
FROM "mongodb_gluedb"."sample_mflix_movies_v3"
WHERE "_id" = '<E2E_ID>'
GROUP BY "_id";
```

정상 결과는 `row_count=1`입니다.

v3 전체:

```sql
SELECT
    "_id",
    count(*) AS row_count
FROM "mongodb_gluedb"."sample_mflix_movies_v3"
GROUP BY "_id"
HAVING count(*) <> 1;
```

정상 결과는 `0건`입니다.

### 13.8 `deletedAt` 선택 검증

Schema 확인:

```sql
DESCRIBE "mongodb_gluedb"."sample_mflix_movies_v3";
```

`deletedAt` 컬럼이 존재한다면 다음을 확인합니다.

```sql
SELECT
    "_id",
    "deletedAt"
FROM "mongodb_gluedb"."sample_mflix_movies_v3"
WHERE "_id" = '<E2E_ID>';
```

`deletedAt IS NOT NULL`이어야 합니다.

`deletedAt`이 Schema에 없으면 Soft Delete 핵심 기능과 별도로 Schema Evolution을 추가 확인해야 합니다. 존재하지 않는 컬럼을 SELECT하면 `COLUMN_NOT_FOUND`가 발생합니다.

## 14. Checkpoint 복구 테스트

기본 E2E 테스트가 성공한 후 수행합니다.

### 14.1 Processor 중지

ASP Workspace:

```javascript
sp.sp01.stop()
```

`sp.listStreamProcessors()`에서 `state=STOPPED`를 확인합니다.

### 14.2 중지 중 이벤트 생성

소스 Atlas 클러스터:

```javascript
var sourceDb = db.getSiblingDB("sample_mflix")
var checkpointId = ObjectId()
var checkpointIdString = checkpointId.toHexString()

sourceDb.movies.insertOne({
  _id: checkpointId,
  title: "ASP_CHECKPOINT_" + checkpointIdString,
  year: NumberInt(2099),
  runtime: NumberInt(300),
  type: "movie"
})

sourceDb.movies.updateOne(
  { _id: checkpointId },
  {
    $set: {
      runtime: NumberInt(320)
    }
  }
)

sourceDb.movies.deleteOne({
  _id: checkpointId
})

print(checkpointIdString)
```

### 14.3 기존 Checkpoint로 재시작

```javascript
sp.sp01.start()
```

이 테스트에서 `resumeFromCheckpoint=false` 또는 `clearCheckpoints=true`를 사용하면 안 됩니다.

### 14.4 결과 확인

```sql
SELECT
    "_id",
    runtime,
    "isDeleted",
    "operationType"
FROM "mongodb_gluedb"."sample_mflix_movies_v3"
WHERE "_id" = '<CHECKPOINT_ID>';
```

성공 조건:

```text
row_count = 1
runtime = 320
isDeleted = true
operationType = delete
```

추가 조건:

- DLQ Delta가 `0`입니다.
- Processor가 `STARTED` 상태입니다.
- Checkpoint 시각이 갱신됩니다.

## 15. 데이터 수량 대사

MongoDB 활성 문서 수:

```javascript
db.getSiblingDB("sample_mflix")
  .movies
  .countDocuments({})
```

Iceberg 활성 행 수:

```sql
SELECT count(*) AS active_count
FROM "mongodb_gluedb"."sample_mflix_movies_v3"
WHERE "isDeleted" = false;
```

처리 지연이 없는 동일 시점 기준으로 두 수량이 일치해야 합니다.

Iceberg 전체 행 수는 Soft Delete 이력이 남기 때문에 MongoDB 현재 문서 수보다 클 수 있습니다.

## 16. 오류 진단

| 증상 | 원인 | 조치 |
| --- | --- | --- |
| `$emit` 성공, `$iceberg` Metadata 생성 실패 | 점이 포함된 버킷 또는 Iceberg 전용 권한 문제 | 점 없는 버킷과 S3/Glue 권한 확인 |
| `Cannot determine whether commit was successful` | Iceberg Commit 결과 불명확 | 동일 대상 재시도 중단, S3/Glue 확인 후 새 Path/Table 사용 |
| `resumeFromCheckpoint should be false` | Initial Sync 중 Checkpoint 유지 수정 | 새 테이블 최초 구축에만 `false` 사용 |
| Pipeline Stage는 한 필드만 가져야 함 | `{name,pipeline,tier}` 전체 전달 | 내부 `.pipeline` 배열만 전달 |
| `stats.status`가 `undefined` | 현재 응답에서 상태가 최상위 필드 | `result.state` 사용 |
| Athena 결과 0건 | Initial Sync 진행, Commit 지연, DLQ 또는 잘못된 테이블/ID | Source targetStats, DLQ Delta, Snapshot과 실제 `$iceberg` 대상 확인 |
| `COLUMN_NOT_FOUND: deletedat` | `deletedAt`이 아직 Iceberg Schema에 없음 | `DESCRIBE`, Pipeline의 `deletedAt:null` 제거, 새 Delete로 Schema Evolution 확인 |
| `_id` 중복 | 반복 Initial Sync 또는 at-least-once 재처리 | 새 Table 재구축, 같은 테이블에 `resumeFromCheckpoint=false` 반복 금지 |
| Delete 후 행 0건 | Delete가 Iceberg 물리 삭제로 처리됨 | `$setStreamMeta`와 `stream.source.operationType=update` 확인 |
| Delete 후 `false/insert` 유지 | Delete 미처리 또는 Commit 대기 | Source count, Output/DLQ Delta, Snapshot 확인 |
| DLQ 증가 | Pre Image, 타입, Schema 또는 외부 저장소 오류 | `_dlqMessage.errInfo.reason` 확인 |

## 17. 운영 주의사항

- ASP의 Iceberg 출력은 at-least-once 처리이므로 중복 검증이 필요합니다.
- 같은 Iceberg 테이블에 Initial Sync를 반복하지 않습니다.
- `resumeFromCheckpoint=false`는 새 테이블 전체 재구축에만 사용합니다.
- 일반 재시작은 기존 Checkpoint를 사용합니다.
- Oplog Window보다 오래 Processor가 중지되면 Checkpoint 복구가 실패할 수 있습니다.
- Pre/Post Image가 없거나 만료되면 Delete 이전 필드를 보존할 수 없습니다.
- ASP를 Iceberg 테이블의 단일 Writer로 운영하는 것을 권장합니다.
- Snapshot 만료, 작은 파일 병합과 orphan file 정리 정책이 필요합니다.
- Iceberg 경로에 S3 Lifecycle 삭제를 직접 적용하지 않습니다.
- DLQ 증가와 Processor 실패에 Alert를 구성합니다.

## 18. 최종 체크리스트

### 설정

- [ ] 점 없는 S3 버킷 사용
- [ ] S3 Public Access 차단
- [ ] SSE-S3 또는 KMS 권한 확인
- [ ] IAM Trust Policy와 External ID 확인
- [ ] S3 읽기/쓰기/삭제 권한 확인
- [ ] Glue Database/Table 권한 확인
- [ ] Atlas Source Connection 확인
- [ ] ASP S3 Connection 확인
- [ ] Change Stream Pre/Post Image 활성화
- [ ] Oplog Window 확인
- [ ] `$iceberg.path` 후행 `/` 제거
- [ ] SP10 이상 Tier 사용

### Mock 테스트

- [ ] `$emit` S3 파일 생성 확인
- [ ] 최소 `$iceberg` Metadata/Data 생성 확인
- [ ] Glue Mock Table 생성 확인
- [ ] Athena Mock 문서 조회 확인

### 전체 테스트

- [ ] Processor `STARTED` 확인
- [ ] Initial Sync `completed` 확인
- [ ] Insert 한 행 확인
- [ ] Update 최신 값 확인
- [ ] MongoDB Delete 후 원본 `null` 확인
- [ ] Iceberg Soft Delete 한 행 확인
- [ ] `isDeleted=true` 확인
- [ ] `operationType=delete` 확인
- [ ] Delete 이전 필드 보존 확인
- [ ] 테스트 전후 DLQ 증가 없음 확인
- [ ] `_id` 중복 없음 확인
- [ ] Checkpoint 복구 확인
- [ ] 활성 데이터 수량 대사
- [ ] `deletedAt` 요구 시 Schema와 값 확인

## 19. 현재 남은 운영 과제

핵심 Insert/Delete/Soft Delete 기능은 `v3`에서 성공했습니다. 운영 전 다음 항목은 추가로 완료해야 합니다.

- 과거 테스트에서 누적된 DLQ 70건의 원인 분류
- `v3` 전체 `_id` 중복 조회
- `v3` Update 테스트
- Checkpoint 중지/재시작 복구 테스트
- `deletedAt` 컬럼이 요구사항이면 Schema Evolution 확인
- DLQ 및 Processor 상태 Alert 구성
- Iceberg Snapshot/파일 유지보수 정책 수립

## 20. 참고 문서

- [MongoDB Atlas Stream Processing](https://www.mongodb.com/docs/atlas/atlas-stream-processing/)
- [Atlas Stream Processing `$source`](https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-source/)
- [Atlas Stream Processing `$iceberg`](https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-iceberg/)
- [Atlas Stream Processing `$setStreamMeta`](https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-setStreamMeta/)
- [Develop and Manage Stream Processors](https://www.mongodb.com/docs/atlas/atlas-stream-processing/manage-stream-processor/)
- [Amazon S3 Bucket Naming Rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)
- [Amazon Athena Apache Iceberg](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
