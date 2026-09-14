# Atlas Stream Processing to Iceberg End-to-End 검증 결과

## 1. 문서 목적

이 문서는 MongoDB Atlas `sample_mflix.movies` 컬렉션을 Atlas Stream Processing(ASP)을 통해 AWS S3의 Apache Iceberg 테이블로 적재하고, Initial Sync와 Insert/Update/Delete CDC를 검증한 과정을 정리합니다.

검증 결과는 다음과 같습니다.

- 점(`.`)이 없는 새 S3 버킷으로 Iceberg Metadata 및 Data 파일 생성 성공
- AWS Glue Data Catalog 테이블 생성 성공
- 기존 데이터 Initial Sync 성공
- Insert 반영 성공
- Update 반영 성공
- Delete 이벤트의 Soft Delete 변환 성공
- Athena에서 최종 데이터 조회 성공

## 2. 최종 구성

| 항목 | 값 |
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
| Iceberg Path | `sample-mflix-v2` |
| Glue Database | `mongodb_gluedb` |
| Iceberg Table | `sample_mflix_movies_v2` |

전체 흐름은 다음과 같습니다.

```text
MongoDB Atlas sample_mflix.movies
        |
        | Initial Sync + Change Stream
        v
Atlas Stream Processing sp01
        |
        | Unified AWS Access / STS AssumeRole
        v
Amazon S3
s3://sumi-asp-iceberg-bucket/sample-mflix-v2/...
        |
        +--- AWS Glue Data Catalog
        |    mongodb_gluedb.sample_mflix_movies_v2
        |
        v
Amazon Athena
```

## 3. 기존 오류와 원인

S3 `$emit` 테스트는 성공했지만 기존 버킷을 사용한 최소 `$iceberg` 테스트는 다음 Metadata 파일 생성 단계에서 실패했습니다.

```text
Failed to create file:
s3://sumi.bucket01-979559056307-ap-northeast-2-an/
iceberg-smoke-test/mongodb_gluedb/iceberg_smoke_test/metadata/...
```

기존 버킷 이름에는 점(`.`)이 포함되어 있었습니다.

```text
sumi.bucket01-979559056307-ap-northeast-2-an
```

S3는 점을 허용하지만 HTTPS virtual-host 방식의 TLS 인증서 및 일부 S3 Client 구현과 호환 문제가 발생할 수 있습니다. Iceberg S3 FileIO와의 호환성을 위해 점이 없는 새 버킷을 사용했습니다.

```text
sumi-asp-iceberg-bucket
```

기존 오류에는 Commit 성공 여부가 불확실하다는 메시지도 포함되어 있었습니다. 동일 대상에 재시도할 때 발생할 수 있는 중복 또는 손상을 피하기 위해 새 Path와 Table을 사용했습니다.

```text
기존: sample-mflix / sample_mflix_movies
신규: sample-mflix-v2 / sample_mflix_movies_v2
```

기존 버킷과 Glue 테이블은 Commit 및 orphan file 확인이 끝날 때까지 바로 삭제하지 않습니다.

## 4. 새 S3 버킷 생성 기준

새 버킷 생성 시 다음 항목을 확인합니다.

- General purpose 버킷 사용
- 리전은 ASP 및 Glue와 동일한 `ap-northeast-2` 사용
- 버킷 이름에 점(`.`)을 사용하지 않음
- Block all public access 활성화
- Object Ownership은 Bucket owner enforced 사용
- 기본 암호화는 SSE-S3 사용
- 버전 관리는 운영 정책에 따라 활성화 권장
- 테스트에서는 Object Lock 비활성화
- Iceberg 관리 경로에 임의의 S3 Lifecycle 삭제 정책을 적용하지 않음

S3 버킷은 생성 후 이름과 리전을 변경할 수 없습니다. 이름을 변경해야 하면 새 버킷을 만들어야 합니다.

## 5. AWS IAM 권한 변경

Atlas Connection `sumi-con-iceberg`가 사용하는 IAM Role의 S3 Resource를 새 버킷 ARN으로 변경했습니다.

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

동일 IAM Role을 계속 사용했으므로 Atlas의 `connectionName`은 변경하지 않았습니다.

```javascript
connectionName: "sumi-con-iceberg"
```

SSE-KMS를 사용한다면 IAM Policy 외에 KMS Key Policy와 다음 권한도 필요합니다.

```text
kms:Encrypt
kms:Decrypt
kms:GenerateDataKey
kms:DescribeKey
```

## 6. Change Stream Pre/Post Image

Delete 이벤트에서 삭제 전 문서를 보존하기 위해 `movies` 컬렉션의 Change Stream Pre/Post Image를 활성화합니다. 이 명령은 ASP Workspace가 아닌 소스 Atlas 클러스터에서 실행합니다.

```javascript
db.getSiblingDB("sample_mflix").runCommand({
  collMod: "movies",
  changeStreamPreAndPostImages: {
    enabled: true
  }
})
```

설정 확인:

```javascript
db.getSiblingDB("sample_mflix")
  .getCollectionInfos({ name: "movies" })[0]
  .options
  .changeStreamPreAndPostImages
```

`fullDocumentBeforeChange: "required"`인데 Pre Image를 사용할 수 없으면 Stream Processor가 실패합니다.

## 7. 검증된 ASP 파이프라인

```javascript
var pipelineOnly = [
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
                deletedAt: null,
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
      path: "sample-mflix-v2",
      databaseName: "mongodb_gluedb",
      tableName: "sample_mflix_movies_v2",
      catalog: {
        type: "glue"
      },
      mode: "cdc",
      idFieldName: "_id"
    }
  }
]
```

Delete 이벤트는 데이터 필드에 `isDeleted=true`, `operationType="delete"`를 기록합니다. 동시에 `$setStreamMeta`에서 Iceberg가 실행할 CDC 작업을 `delete`가 아닌 `update`로 변경합니다. 따라서 Iceberg 행은 물리적으로 삭제되지 않고 Soft Delete 상태로 갱신됩니다.

## 8. 기존 Processor 수정

`sp.listStreamProcessors()`의 결과는 Processor 정보가 포함된 배열입니다.

```javascript
[
  {
    name: "sp01",
    pipeline: [/* stages */],
    tier: "SP10"
  }
]
```

이 전체 객체를 `modify()`의 Pipeline으로 전달하면 다음 오류가 발생합니다.

```text
A pipeline stage specification object must contain exactly one field
```

Pipeline에는 `name`과 `tier`가 아니라 실제 Stage 배열만 전달해야 합니다.

기존 Processor에서 Pipeline을 가져오는 방법:

```javascript
var processorDefinition = sp.listStreamProcessors()
  .find(item => item.name === "sp01")

var pipelineOnly = processorDefinition.pipeline
```

Iceberg Stage를 찾아 새 대상을 설정합니다.

```javascript
var icebergStage = pipelineOnly.find(stage => stage.$iceberg)

icebergStage.$iceberg.bucket = "sumi-asp-iceberg-bucket"
icebergStage.$iceberg.path = "sample-mflix-v2"
icebergStage.$iceberg.tableName = "sample_mflix_movies_v2"
```

Processor가 실행 중이면 중지합니다.

```javascript
sp.sp01.stop()
```

Initial Sync 중인 Processor는 기존 체크포인트를 유지하면서 수정할 수 없습니다. Pipeline 배열과 옵션을 분리하여 `resumeFromCheckpoint: false`로 수정합니다.

```javascript
sp.sp01.modify(
  pipelineOnly,
  { resumeFromCheckpoint: false }
)
```

성공 결과:

```javascript
{ ok: 1 }
```

Processor를 다시 시작합니다.

```javascript
sp.sp01.start()
```

성공 결과:

```javascript
{ ok: 1 }
```

`resumeFromCheckpoint: false`를 사용하면 기존 Initial Sync 진행 상태를 버리고 처음부터 다시 동기화합니다. 대상 테이블이 기존 데이터와 겹치면 중복 또는 예기치 않은 변경이 발생할 수 있으므로 새 Path와 Table을 사용합니다.

## 9. Initial Sync 확인

ASP Workspace의 `mongosh`에서 다음 명령을 실행합니다.

```javascript
sp.sp01.stats({
  options: { verbose: true }
}).stats.operatorStats[0].targetStats
```

진행 중인 상태:

```javascript
initialSync: {
  estimatedDocs: Long("..."),
  copiedDocs: Long("..."),
  status: "in_progress"
}
```

완료 상태:

```javascript
initialSync: {
  estimatedDocs: Long("..."),
  copiedDocs: Long("..."),
  status: "completed"
}
```

수동 CDC 테스트는 Initial Sync가 완료된 후 진행하는 것이 결과를 구분하기 쉽습니다.

## 10. S3와 Glue 확인

S3에서 다음 구조가 생성되는지 확인합니다.

```text
s3://sumi-asp-iceberg-bucket/
  sample-mflix-v2/
    mongodb_gluedb/
      sample_mflix_movies_v2/
        metadata/
        data/
```

Glue Data Catalog에서 다음 테이블을 확인합니다.

```text
Database: mongodb_gluedb
Table: sample_mflix_movies_v2
```

AWS CLI 확인 예시:

```bash
aws glue get-table \
  --database-name mongodb_gluedb \
  --name sample_mflix_movies_v2 \
  --region ap-northeast-2
```

## 11. Insert, Update, Delete 검증

다음 명령은 ASP Workspace가 아닌 소스 Atlas 클러스터에 연결한 `mongosh`에서 실행합니다.

### 11.1 Insert

```javascript
use sample_mflix

var testId = ObjectId()

db.movies.insertOne({
  _id: testId,
  title: "ASP ICEBERG CDC TEST",
  year: 2026,
  plot: "Insert test",
  type: "movie"
})

testId.toString()
```

출력된 24자리 ObjectId 문자열을 기록합니다. ObjectId는 Iceberg에서 문자열로 저장됩니다.

Athena 확인:

```sql
SELECT
    "_id",
    title,
    year,
    "isDeleted",
    "operationType"
FROM "mongodb_gluedb"."sample_mflix_movies_v2"
WHERE "_id" = '<테스트 ObjectId>';
```

기대 결과:

```text
행 개수 = 1
title = ASP ICEBERG CDC TEST
year = 2026
isDeleted = false
operationType = insert
```

### 11.2 Update

```javascript
db.movies.updateOne(
  { _id: testId },
  {
    $set: {
      title: "ASP ICEBERG CDC TEST UPDATED",
      year: 2027
    }
  }
)
```

Athena에서 동일한 `_id`를 조회합니다.

기대 결과:

```text
행 개수 = 1
title = ASP ICEBERG CDC TEST UPDATED
year = 2027
isDeleted = false
operationType = update
```

### 11.3 Delete와 Soft Delete 상세 검증

Delete 테스트는 다음 네 가지를 모두 확인해야 성공입니다.

1. MongoDB 원본 문서가 실제로 삭제됩니다.
2. Stream Processor가 실패하지 않고 Delete 이벤트를 출력합니다.
3. Iceberg에는 동일한 `_id`의 행이 정확히 한 건 남습니다.
4. 남은 행이 `isDeleted=true`, `operationType=delete`, `deletedAt IS NOT NULL` 상태입니다.

테스트는 운영 데이터와 구분되는 새로운 ObjectId를 사용합니다. Soft Delete 행은 Iceberg에 의도적으로 남으므로 테스트 전용 테이블에서 수행하는 것을 권장합니다.

#### Step 1. Processor와 대상 설정 확인

ASP Workspace의 `mongosh`에서 실행합니다.

```javascript
var spDefinition = sp.listStreamProcessors()
  .find(item => item.name === "sp01")

var sourceStage = spDefinition.pipeline
  .find(stage => stage.$source)

var icebergStage = spDefinition.pipeline
  .find(stage => stage.$iceberg)

var setStreamMetaStage = spDefinition.pipeline
  .find(stage => stage.$setStreamMeta)

printjson({
  state: spDefinition.state,
  source: sourceStage,
  iceberg: icebergStage,
  hasSetStreamMeta: Boolean(setStreamMetaStage)
})
```

다음 항목을 확인합니다.

```text
state = STARTED
$source.initialSync.enable = true
$source.config.fullDocumentBeforeChange = required
$iceberg.bucket = sumi-asp-iceberg-bucket
$iceberg.tableName = sample_mflix_movies_v2
$iceberg.mode = cdc
$iceberg.idFieldName = _id
hasSetStreamMeta = true
```

#### Step 2. Pre/Post Image 확인

이 단계부터는 ASP Workspace가 아닌 소스 Atlas 클러스터의 `mongosh`에서 실행합니다.

```javascript
var sourceDb = db.getSiblingDB("sample_mflix")

var collInfo = sourceDb
  .getCollectionInfos({ name: "movies" })[0]

printjson(collInfo.options.changeStreamPreAndPostImages)

if (
  !collInfo.options.changeStreamPreAndPostImages ||
  collInfo.options.changeStreamPreAndPostImages.enabled !== true
) {
  throw new Error("movies 컬렉션의 Pre/Post Image가 활성화되지 않았습니다.")
}
```

기대 결과:

```javascript
{
  enabled: true
}
```

활성화되지 않았다면 테스트 전에 실행합니다.

```javascript
sourceDb.runCommand({
  collMod: "movies",
  changeStreamPreAndPostImages: {
    enabled: true
  }
})
```

Pre/Post Image를 활성화하기 전에 생성된 Change Event에는 Pre Image가 없을 수 있습니다. 활성화 후 새로운 테스트 문서를 생성해야 합니다.

#### Step 3. 독립적인 Delete 테스트 문서 생성

소스 Atlas 클러스터의 동일한 `mongosh` 세션에서 실행합니다.

```javascript
var softDeleteTestId = ObjectId()
var softDeleteTestIdString = softDeleteTestId.toHexString()
var softDeleteTestTitle = "ASP_SOFT_DELETE_" + softDeleteTestIdString

var insertResult = sourceDb.movies.insertOne({
  _id: softDeleteTestId,
  title: softDeleteTestTitle,
  year: NumberInt(2099),
  runtime: NumberInt(777),
  plot: "ASP Iceberg soft delete verification",
  type: "movie"
})

if (!insertResult.acknowledged) {
  throw new Error("테스트 문서 Insert가 승인되지 않았습니다.")
}

printjson({
  testId: softDeleteTestIdString,
  expectedTitle: softDeleteTestTitle,
  insertResult: insertResult
})
```

출력된 `testId`와 `expectedTitle`을 기록합니다. 이후 Athena 쿼리의 `<TEST_OBJECT_ID>`와 `<EXPECTED_TITLE>`에 사용합니다.

#### Step 4. Delete 전 Iceberg 기준 상태 확인

Delete 전에 Insert가 Iceberg에 반영됐는지 Athena에서 확인합니다. Insert가 확인되기 전에 바로 삭제하면 Insert 실패와 Delete 실패를 구분하기 어렵습니다.

```sql
SELECT
    "_id",
    title,
    year,
    runtime,
    "isDeleted",
    "deletedAt",
    "operationType"
FROM "mongodb_gluedb"."sample_mflix_movies_v2"
WHERE "_id" = '<TEST_OBJECT_ID>';
```

Delete 전 기대 결과:

```text
행 개수 = 1
title = <EXPECTED_TITLE>
year = 2099
runtime = 777
isDeleted = false
deletedAt = null
operationType = insert
```

결과가 아직 없으면 잠시 기다린 후 같은 쿼리를 다시 실행합니다. Processor가 비동기로 Iceberg Snapshot을 Commit하므로 MongoDB Insert 직후에는 보이지 않을 수 있습니다.

#### Step 5. Delete 전 Processor 상태 기록

ASP Workspace의 `mongosh`에서 실행합니다.

```javascript
var beforeDeleteStats = sp.sp01.stats({
  options: { verbose: true }
})

printjson({
  status: beforeDeleteStats.stats.status,
  outputMessageCount: beforeDeleteStats.stats.outputMessageCount,
  dlqMessageCount: beforeDeleteStats.stats.dlqMessageCount
})
```

`status`가 `running`이고 `dlqMessageCount`가 `0`인지 확인합니다. 운영 트래픽이 함께 처리되는 환경에서는 `outputMessageCount`의 정확한 증가량보다 삭제 후에도 값이 증가하고 Processor가 정상 상태를 유지하는지가 중요합니다.

#### Step 6. MongoDB Delete 실행과 즉시 검증

다시 소스 Atlas 클러스터의 `mongosh`에서 실행합니다. Step 3과 다른 세션이라면 기록한 ObjectId로 변수를 다시 만듭니다.

```javascript
// 새 세션에서만 실제 테스트 ObjectId로 다시 설정합니다.
// var sourceDb = db.getSiblingDB("sample_mflix")
// var softDeleteTestId = ObjectId("<TEST_OBJECT_ID>")

var deleteRequestedAt = new Date()

var deleteResult = sourceDb.movies.deleteOne({
  _id: softDeleteTestId
})

if (deleteResult.deletedCount !== 1) {
  throw new Error(
    "Delete 대상이 한 건이 아닙니다. deletedCount=" +
    deleteResult.deletedCount
  )
}

var sourceDocumentAfterDelete = sourceDb.movies.findOne({
  _id: softDeleteTestId
})

if (sourceDocumentAfterDelete !== null) {
  throw new Error("MongoDB 원본 문서가 삭제되지 않았습니다.")
}

printjson({
  testId: softDeleteTestId.toHexString(),
  deleteRequestedAt: deleteRequestedAt,
  deletedCount: deleteResult.deletedCount,
  sourceDocumentAfterDelete: sourceDocumentAfterDelete
})
```

기대 결과:

```text
deletedCount = 1
sourceDocumentAfterDelete = null
```

여기까지는 MongoDB 원본 Delete 성공만 의미합니다. Iceberg Soft Delete 성공 여부는 다음 단계에서 별도로 확인해야 합니다.

#### Step 7. Delete 후 Processor와 DLQ 확인

ASP Workspace의 `mongosh`에서 실행합니다.

```javascript
var afterDeleteStats = sp.sp01.stats({
  options: { verbose: true }
})

printjson({
  status: afterDeleteStats.stats.status,
  outputMessageCount: afterDeleteStats.stats.outputMessageCount,
  dlqMessageCount: afterDeleteStats.stats.dlqMessageCount
})
```

성공 조건:

```text
status = running
outputMessageCount = Delete 전보다 증가
dlqMessageCount = Delete 전과 동일하며 정상적으로는 0
```

`status=error`이거나 DLQ가 증가하면 다음 항목을 확인합니다.

- `fullDocumentBeforeChange: "required"` 설정
- `changeStreamPreAndPostImages.enabled=true` 설정
- Pre Image 보존 기간
- `$setStreamMeta`의 `stream.source.operationType` 변환
- S3, Glue 또는 KMS 권한 오류

#### Step 8. Athena에서 최종 Soft Delete 행 확인

Iceberg Commit 후 다음 쿼리를 실행합니다.

```sql
SELECT
    "_id",
    title,
    year,
    runtime,
    "isDeleted",
    "deletedAt",
    "operationType"
FROM "mongodb_gluedb"."sample_mflix_movies_v2"
WHERE "_id" = '<TEST_OBJECT_ID>';
```

최종 기대 결과:

```text
행 개수 = 1
title = <EXPECTED_TITLE>
year = 2099
runtime = 777
isDeleted = true
deletedAt = MongoDB 삭제 시각
operationType = delete
```

이 결과는 다음 동작을 증명합니다.

- `title`, `year`, `runtime` 유지: `fullDocumentBeforeChange`가 정상적으로 전달됨
- `isDeleted=true`: Delete 문서 변환 성공
- `deletedAt` 존재: Delete 이벤트 시간이 기록됨
- `operationType=delete`: 원본 작업 유형이 데이터에 보존됨
- 행 개수 1: Iceberg에서 물리 삭제되지 않았고 중복도 발생하지 않음

#### Step 9. Athena 자동 PASS/FAIL 판정

다음 쿼리는 주요 조건을 한 번에 검사합니다. 두 Placeholder를 Step 3에서 기록한 값으로 바꿉니다.

```sql
WITH target AS (
    SELECT *
    FROM "mongodb_gluedb"."sample_mflix_movies_v2"
    WHERE "_id" = '<TEST_OBJECT_ID>'
)
SELECT
    count(*) AS row_count,
    sum(CASE WHEN "isDeleted" = true THEN 1 ELSE 0 END)
        AS soft_deleted_count,
    sum(CASE WHEN "deletedAt" IS NOT NULL THEN 1 ELSE 0 END)
        AS deleted_at_count,
    sum(CASE WHEN "operationType" = 'delete' THEN 1 ELSE 0 END)
        AS delete_operation_count,
    sum(CASE WHEN title = '<EXPECTED_TITLE>' THEN 1 ELSE 0 END)
        AS preserved_title_count,
    sum(CASE WHEN year = 2099 AND runtime = 777 THEN 1 ELSE 0 END)
        AS preserved_value_count,
    CASE
        WHEN count(*) = 1
         AND sum(CASE WHEN "isDeleted" = true THEN 1 ELSE 0 END) = 1
         AND sum(CASE WHEN "deletedAt" IS NOT NULL THEN 1 ELSE 0 END) = 1
         AND sum(CASE WHEN "operationType" = 'delete' THEN 1 ELSE 0 END) = 1
         AND sum(CASE WHEN title = '<EXPECTED_TITLE>' THEN 1 ELSE 0 END) = 1
         AND sum(CASE WHEN year = 2099 AND runtime = 777 THEN 1 ELSE 0 END) = 1
        THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result
FROM target;
```

정상 결과:

```text
row_count = 1
soft_deleted_count = 1
deleted_at_count = 1
delete_operation_count = 1
preserved_title_count = 1
preserved_value_count = 1
test_result = PASS
```

Athena 또는 Glue에서 컬럼명이 소문자로 정규화되어 있다면 먼저 다음 명령으로 실제 컬럼명을 확인하고 쿼리의 `isDeleted`, `deletedAt`, `operationType`을 각각 실제 이름으로 바꿉니다.

```sql
DESCRIBE "mongodb_gluedb"."sample_mflix_movies_v2";
```

#### Step 10. 실패 결과 해석

| 결과 | 의미 | 확인할 항목 |
| --- | --- | --- |
| MongoDB `deletedCount=0` | 원본 테스트 문서가 없거나 ID가 다름 | `testId`, 대상 DB/Collection 확인 |
| MongoDB에는 없고 Iceberg `row_count=0` | Iceberg에서도 물리 삭제됐거나 Insert가 반영되지 않음 | `$setStreamMeta`, Delete 전 기준 조회 확인 |
| Iceberg `row_count=1`, `isDeleted=false` | Delete 이벤트 미처리 또는 Commit 대기 중 | Processor 출력 건수, 상태, Glue Snapshot 재조회 |
| Iceberg `row_count=1`, `isDeleted=true`, `deletedAt=null` | Delete 시간 변환 누락 | `$replaceRoot`의 `deletedAt: "$wallTime"` 확인 |
| Iceberg `row_count>1` | CDC 키 또는 중복 처리 문제 | `mode: "cdc"`, `idFieldName: "_id"`, 체크포인트 확인 |
| `title/year/runtime` 유실 | Delete Pre Image를 받지 못함 | Pre/Post Image 설정 및 보존 기간 확인 |
| Processor `status=error` | 파이프라인 또는 외부 저장소 오류 | Processor 오류 메시지, S3/Glue/KMS 권한 확인 |
| DLQ 증가 | 해당 이벤트 변환 또는 스키마 오류 | DLQ 문서의 `reason`과 원본 문서 확인 |

Iceberg Commit은 비동기로 반영될 수 있습니다. 최종 쿼리가 처음에 `FAIL`이면 Processor가 정상 실행 중인지 확인하면서 일정 간격으로 다시 실행합니다. Processor 출력 건수가 증가했는데도 계속 `FAIL`이면 단순 지연이 아닌 파이프라인 또는 Iceberg Commit 문제로 판단합니다.

## 12. 최종 검증 결과

| 검증 항목 | 결과 |
| --- | --- |
| 새 S3 버킷 접근 | 성공 |
| Iceberg Metadata 생성 | 성공 |
| Iceberg Data 생성 | 성공 |
| Glue Table 생성 | 성공 |
| Processor 수정 | 성공 |
| Processor 시작 | 성공 |
| Initial Sync | 성공 |
| Insert CDC | 성공 |
| Update CDC | 성공 |
| Delete Soft Delete | 성공 |
| Athena 조회 | 성공 |

## 13. 오류별 해결 내용

| 오류 | 원인 | 해결 |
| --- | --- | --- |
| `Failed to create file ... metadata.json` | 점이 포함된 S3 버킷과 Iceberg S3 FileIO 호환 가능성 | 점이 없는 새 버킷 사용 |
| `Cannot determine whether the commit was successful` | Iceberg Commit 결과 불명확 | 동일 대상 재시도 중단, S3/Glue 확인 후 새 Path/Table 사용 |
| `resumeFromCheckpoint should be false` | Initial Sync 체크포인트를 유지한 상태로 Processor 수정 | `modify(pipelineOnly, { resumeFromCheckpoint: false })` 사용 |
| `A pipeline stage specification object must contain exactly one field` | `{ name, pipeline, tier }` 전체를 Pipeline으로 전달 | 내부 `.pipeline` 배열만 전달 |
| `pipeline is not defined` | 현재 `mongosh` 세션에 Pipeline 변수가 없음 | `sp.listStreamProcessors()`에서 `.pipeline` 추출 |
| Delete 또는 Update 시 Processor 실패 | Pre/Post Image 미설정 또는 만료 | `changeStreamPreAndPostImages.enabled=true` 확인 |

## 14. 운영 전 체크리스트

- [x] 점이 없는 S3 버킷 사용
- [x] S3 Public Access 차단
- [x] IAM Role의 새 S3 ARN 반영
- [x] Iceberg에 필요한 S3 읽기/쓰기/삭제 권한 확인
- [x] Glue Database/Table 권한 확인
- [x] Change Stream Pre/Post Image 활성화
- [x] `$iceberg.path` 후행 `/` 제거
- [x] 새 Path와 Table로 재구축
- [x] Initial Sync 완료 확인
- [x] Insert/Update/Delete 테스트 완료
- [x] Soft Delete 후 Iceberg 행 한 건 유지 확인
- [x] Athena 조회 성공
- [ ] Processor 실패 및 DLQ Alert 구성
- [ ] Atlas와 Iceberg 활성 데이터 정기 수량 대사
- [ ] Iceberg Snapshot 만료 및 orphan file 정리 정책 수립
- [ ] Iceberg 데이터 경로의 S3 Lifecycle 정책 검토

## 15. 참고 문서

- [Atlas Stream Processing](https://www.mongodb.com/docs/atlas/atlas-stream-processing/)
- [Atlas Stream Processing `$source`](https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-source/)
- [Atlas Stream Processing `$iceberg`](https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-iceberg/)
- [Atlas Stream Processing `$setStreamMeta`](https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-setStreamMeta/)
- [Develop and Manage Stream Processors](https://www.mongodb.com/docs/atlas/atlas-stream-processing/manage-stream-processor/)
- [Amazon S3 Bucket Naming Rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)
- [Amazon Athena Apache Iceberg](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
