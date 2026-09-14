# Atlas Stream Processing용 AWS Iceberg IAM 구성 가이드

## 1. 문서 목적

이 문서는 MongoDB Atlas Stream Processing(ASP)이 고객 AWS 계정의 S3 및 AWS Glue Data Catalog에 Apache Iceberg 데이터를 쓸 수 있도록 IAM Role을 구성하는 방법을 설명합니다.

다음 사용 사례를 기준으로 합니다.

- MongoDB Atlas의 프로젝트는 MongoDB SA 조직 안에 있습니다.
- ASP가 고객 AWS 계정의 S3 버킷에 Iceberg 파일을 씁니다.
- Iceberg Catalog로 AWS Glue Data Catalog를 사용합니다.
- AWS Access Key와 Secret Key 대신 STS AssumeRole을 사용합니다.
- 최소 권한 원칙에 따라 특정 S3 버킷과 Glue Database만 허용합니다.

## 2. 핵심 결론

`AmazonS3FullAccess`만으로는 충분하지 않습니다.

| 필요한 권한 | `AmazonS3FullAccess` 포함 여부 |
| --- | --- |
| S3 읽기, 쓰기, 삭제 | 포함 |
| AWS Glue Database/Table 관리 | 미포함 |
| Atlas의 `sts:AssumeRole` 허용 | 미포함 |
| SSE-KMS 암호화 키 사용 | 미포함 |
| Lake Formation 권한 | 미포함 |
| Atlas Connection Registry 등록 | 별도 작업 |

`AmazonS3FullAccess`는 계정 내 모든 S3 버킷에 대한 광범위한 권한이므로 운영 환경에서는 권장하지 않습니다.

권장 방식은 다음과 같습니다.

```text
Customer Managed IAM Policy
  + 특정 S3 버킷 및 Prefix
  + 특정 Glue Database 및 Table
  + 특정 KMS Key(사용 시)

IAM Role Trust Policy
  + Atlas AWS Account ARN
  + Atlas Project External ID
  + sts:AssumeRole
```

## 3. 인증 구조

Atlas ASP는 고객의 장기 AWS Access Key를 저장하지 않습니다.

```text
MongoDB Atlas AWS Principal
        |
        | sts:AssumeRole
        | External ID 검증
        v
고객 AWS IAM Role
        |
        +--- S3 Iceberg 파일 접근
        +--- Glue Catalog 접근
        +--- KMS Key 접근(선택)
```

IAM Role에는 서로 다른 두 정책이 존재합니다.

| 정책 | 목적 |
| --- | --- |
| Trust Policy | 누가 IAM Role을 Assume할 수 있는지 정의 |
| Permission Policy | Assume한 Role이 AWS에서 무엇을 할 수 있는지 정의 |

Trust Policy와 Permission Policy가 모두 정상이어야 ASP가 Iceberg 테이블을 사용할 수 있습니다.

## 4. 예제 환경

| 항목 | 예제 값 |
| --- | --- |
| 고객 AWS 계정 ID | `123456789012` |
| AWS 리전 | `ap-northeast-2` |
| S3 버킷 | `my-company-analytics-dev` |
| S3 Prefix | `iceberg/asp_analytics/` |
| Glue Database | `asp_analytics` |
| Iceberg Table | `movies_current` |
| IAM Role | `mongodb-atlas-asp-iceberg-writer` |
| IAM Permission Policy | `MongoDBAtlasASPIcebergWriterPolicy` |
| ASP S3 Connection | `aws_iceberg_s3` |

AWS 계정 ID, 리전, 버킷, Prefix와 Glue Database 이름은 실제 환경에 맞게 변경해야 합니다.

IAM Role은 가능하면 S3 버킷과 Glue Catalog를 소유한 AWS 계정에 생성합니다. 다른 계정에 Role을 만들면 S3 Bucket Policy와 Glue Cross-Account 설정이 추가로 필요합니다.

## 5. 사전 권한

### 5.1 MongoDB Atlas

Unified AWS Access 설정에는 다음 중 하나가 필요합니다.

- `Organization Owner`
- `Project Owner`

ASP Workspace와 Connection Registry 관리에는 다음 권한이 필요합니다.

- `Project Stream Processing Owner`

MongoDB SA 조직을 사용하더라도 IAM Role 연결은 실제 ASP가 위치한 Atlas 프로젝트에서 설정합니다. External ID도 해당 Atlas 프로젝트가 제공한 값을 사용해야 합니다.

### 5.2 AWS

작업자는 다음 AWS 권한을 가지고 있어야 합니다.

- IAM Policy 생성
- IAM Role 생성 및 Trust Policy 수정
- S3 버킷 확인
- Glue Database 생성 또는 확인
- KMS Key Policy 확인
- Lake Formation 권한 관리(사용 시)

## 6. Atlas에서 Unified AWS Access 시작

먼저 Atlas에서 인증 절차를 시작해야 Atlas Principal과 External ID를 받을 수 있습니다.

1. MongoDB SA 조직에서 사용할 Atlas 프로젝트를 선택합니다.
2. `Project Settings`로 이동합니다.
3. `Integrations` 탭을 선택합니다.
4. `AWS IAM Role Access`를 찾습니다.
5. `Configure`를 클릭합니다.
6. 기존 Role이 있다면 버튼 이름이 `Edit`일 수 있습니다.
7. `Authorize an AWS IAM Role`을 클릭합니다.
8. 안내 화면에서 `Next`를 클릭합니다.
9. `Add Trust Relationships to an Existing Role` 또는 새 Role 생성 절차를 선택합니다.

Atlas 화면에서 다음 두 값을 확인하고 안전한 작업 메모에 기록합니다.

```text
Atlas AWS Account ARN
Atlas Assumed Role External ID
```

형태 예시:

```text
Atlas AWS Account ARN:
arn:aws:iam::999999999999:root

Atlas Assumed Role External ID:
65ab12cd34ef567890abcdef
```

위 값은 예시입니다. 실제 Atlas 프로젝트에서 표시되는 값을 사용해야 합니다.

External ID는 Atlas 프로젝트별로 달라질 수 있습니다. MongoDB SA 조직의 다른 프로젝트에서 사용한 값을 복사해 재사용하지 않습니다.

## 7. Glue Database 준비

운영 환경에서는 관리자가 Glue Database를 미리 생성하는 방식을 권장합니다.

1. AWS Console에서 `AWS Glue`로 이동합니다.
2. `Data Catalog`를 선택합니다.
3. `Databases`를 선택합니다.
4. `Add database`를 클릭합니다.
5. Database 이름을 입력합니다.

```text
asp_analytics
```

6. Database를 생성합니다.

Glue Database를 미리 생성하면 ASP Role에서 `glue:CreateDatabase` 권한을 제거할 수 있습니다.

ASP가 Iceberg 테이블을 자동 생성해야 하므로 `glue:CreateTable`은 유지합니다.

## 8. Permission Policy 생성

AWS Console에서 다음 순서로 생성합니다.

1. `IAM`으로 이동합니다.
2. 왼쪽 메뉴에서 `Policies`를 선택합니다.
3. `Create policy`를 클릭합니다.
4. `JSON` 편집기를 선택합니다.
5. 아래 정책을 입력합니다.

### 8.1 권장 최소 권한 정책

다음 정책은 Glue Database를 미리 생성한 운영 환경 기준입니다.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GetIcebergBucketLocation",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::my-company-analytics-dev"
      ]
    },
    {
      "Sid": "ListIcebergPrefix",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-company-analytics-dev"
      ],
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "iceberg/asp_analytics",
            "iceberg/asp_analytics/",
            "iceberg/asp_analytics/*"
          ]
        }
      }
    },
    {
      "Sid": "ManageIcebergObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::my-company-analytics-dev/iceberg/asp_analytics/*"
      ]
    },
    {
      "Sid": "ManageIcebergGlueCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:CreateTable",
        "glue:GetTable",
        "glue:UpdateTable"
      ],
      "Resource": [
        "arn:aws:glue:ap-northeast-2:123456789012:catalog",
        "arn:aws:glue:ap-northeast-2:123456789012:database/asp_analytics",
        "arn:aws:glue:ap-northeast-2:123456789012:table/asp_analytics/*"
      ]
    }
  ]
}
```

### 8.2 Glue Database 자동 생성 정책

ASP가 Glue Database도 생성하게 하려면 Glue 작업에 `glue:CreateDatabase`를 추가합니다.

```json
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
    "arn:aws:glue:ap-northeast-2:123456789012:catalog",
    "arn:aws:glue:ap-northeast-2:123456789012:database/asp_analytics",
    "arn:aws:glue:ap-northeast-2:123456789012:table/asp_analytics/*"
  ]
}
```

`glue:CreateDatabase`는 Glue Catalog에 대한 상대적으로 넓은 생성 권한을 줄 수 있으므로 운영에서는 Database 사전 생성을 권장합니다.

### 8.3 정책 생성 완료

정책 이름을 입력합니다.

```text
MongoDBAtlasASPIcebergWriterPolicy
```

정책 설명 예시:

```text
Allows MongoDB Atlas Stream Processing to write Apache Iceberg data to the designated S3 prefix and Glue database.
```

정책을 생성합니다.

## 9. S3 권한 설명

| 권한 | 사용 목적 |
| --- | --- |
| `s3:GetBucketLocation` | 버킷 리전 확인 |
| `s3:ListBucket` | Iceberg 경로 및 객체 목록 확인 |
| `s3:GetObject` | 기존 데이터와 메타데이터 파일 읽기 |
| `s3:GetObjectVersion` | 버전이 있는 객체 읽기 |
| `s3:PutObject` | 데이터, Manifest, Metadata 파일 생성 |
| `s3:DeleteObject` | 오래된 임시 파일 및 Iceberg 관리 파일 정리 |
| `s3:AbortMultipartUpload` | 실패한 Multipart Upload 정리 |

Soft Delete를 사용하더라도 `s3:DeleteObject`가 필요합니다. 이 권한은 분석 행을 직접 삭제하기 위한 권한이 아니라 Iceberg 파일과 메타데이터 수명 주기를 관리하기 위해 필요합니다.

## 10. IAM Role 생성

### 10.1 권장 방식

AWS Console에서 `Custom trust policy`를 사용하고 Atlas가 제공한 Trust Policy를 그대로 적용하는 방법을 권장합니다.

1. AWS Console에서 `IAM`으로 이동합니다.
2. `Roles`를 선택합니다.
3. `Create role`을 클릭합니다.
4. Trusted entity type에서 `Custom trust policy`를 선택합니다.
5. Atlas가 제공한 Trust Policy를 붙여 넣습니다.

Trust Policy 형식:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<ATLAS_AWS_ACCOUNT_ARN>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<ATLAS_EXTERNAL_ID>"
        }
      }
    }
  ]
}
```

예시 형태:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::999999999999:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "65ab12cd34ef567890abcdef"
        }
      }
    }
  ]
}
```

주의사항:

- `Principal`에는 고객 AWS 계정 ID를 넣지 않습니다.
- MongoDB SA 개인 AWS 계정을 넣지 않습니다.
- Atlas 화면에서 받은 `Atlas AWS Account ARN`을 입력합니다.
- External ID도 Atlas 화면에서 받은 값을 그대로 입력합니다.
- `Action`에는 `sts:AssumeRole`을 사용합니다.

6. `Next`를 클릭합니다.
7. `MongoDBAtlasASPIcebergWriterPolicy`를 검색합니다.
8. 해당 Policy를 선택합니다.
9. `Next`를 클릭합니다.
10. Role 이름을 입력합니다.

```text
mongodb-atlas-asp-iceberg-writer
```

11. 필요한 태그를 추가합니다.

| 태그 | 예제 값 |
| --- | --- |
| `Service` | `MongoDB-Atlas-ASP` |
| `Purpose` | `Iceberg-Writer` |
| `Environment` | `Dev` |
| `Owner` | `DataPlatform` |

12. `Create role`을 클릭합니다.

생성된 고객 IAM Role ARN 예시:

```text
arn:aws:iam::123456789012:role/mongodb-atlas-asp-iceberg-writer
```

## 11. AWS Account 방식

Role 생성 시 Trusted entity type을 `AWS account`로 선택하는 것도 가능하지만 정확한 External ID 설정이 필요합니다.

1. Trusted entity type에서 `AWS account`를 선택합니다.
2. `Another AWS account`를 선택합니다.
3. Atlas ARN에서 12자리 AWS Account ID를 추출합니다.
4. 추출한 Account ID를 입력합니다.
5. `Require external ID`를 활성화합니다.
6. Atlas에서 받은 External ID를 입력합니다.

Atlas ARN이 다음과 같다면:

```text
arn:aws:iam::999999999999:root
```

AWS Account ID 입력값은 다음과 같습니다.

```text
999999999999
```

External ID 입력값 예시:

```text
65ab12cd34ef567890abcdef
```

AWS Account 방식은 Trust Policy의 Principal을 계정의 `root` Principal로 생성할 수 있습니다.

```json
{
  "Principal": {
    "AWS": "arn:aws:iam::999999999999:root"
  }
}
```

Atlas가 특정 IAM Principal ARN을 제공한 경우 계정 전체 Principal보다 권한 범위가 넓어질 수 있습니다. Role 생성 후 `Trust relationships`에서 Atlas가 제공한 정확한 JSON과 비교해야 합니다.

권장 기준:

| 선택 항목 | 사용 여부 |
| --- | --- |
| `Custom trust policy` | 권장 |
| `AWS account` | 사용 가능, 생성 후 정책 검증 필요 |
| `AWS service` | 사용하지 않음 |
| `Web identity` | 사용하지 않음 |
| 고객 자신의 AWS Account ID | Atlas Principal로 사용하지 않음 |

가장 안전한 방법은 Atlas 화면에서 생성된 Custom Trust Policy를 그대로 사용하는 것입니다.

## 12. Trust Policy 검증

Role 생성 후 다음 순서로 확인합니다.

1. AWS IAM에서 생성한 Role을 선택합니다.
2. `Trust relationships` 탭을 선택합니다.
3. `Edit trust policy`를 클릭합니다.
4. Principal과 External ID를 확인합니다.

정상 구조:

```json
{
  "Effect": "Allow",
  "Principal": {
    "AWS": "<Atlas가 제공한 AWS Account ARN>"
  },
  "Action": "sts:AssumeRole",
  "Condition": {
    "StringEquals": {
      "sts:ExternalId": "<Atlas가 제공한 External ID>"
    }
  }
}
```

다음과 같은 전체 공개 Trust Policy는 사용하면 안 됩니다.

```json
{
  "Effect": "Allow",
  "Principal": {
    "AWS": "*"
  },
  "Action": "sts:AssumeRole"
}
```

External ID가 없는 다음 구성도 사용하지 않습니다.

```json
{
  "Effect": "Allow",
  "Principal": {
    "AWS": "arn:aws:iam::999999999999:root"
  },
  "Action": "sts:AssumeRole"
}
```

External ID는 다중 고객을 대신해 Role을 Assume하는 서비스에서 Confused Deputy 문제를 방지합니다.

## 13. Atlas에서 Role 인증 완료

AWS에서 IAM Role을 생성한 후 Atlas 화면으로 돌아갑니다.

1. AWS IAM Role 상세 화면에서 고객 Role ARN을 복사합니다.

```text
arn:aws:iam::123456789012:role/mongodb-atlas-asp-iceberg-writer
```

2. Atlas의 `Enter the Role ARN` 필드에 붙여 넣습니다.
3. `Validate and Finish`를 클릭합니다.
4. Role 상태가 `Authorized` 또는 `Ready`인지 확인합니다.

세 가지 ARN과 ID를 혼동하지 않아야 합니다.

| 값 | 입력 위치 |
| --- | --- |
| Atlas AWS Account ARN | AWS Trust Policy의 `Principal` |
| Atlas External ID | AWS Trust Policy의 `sts:ExternalId` |
| 고객 IAM Role ARN | Atlas `Enter the Role ARN` |

Atlas에 입력하는 값은 Atlas Principal ARN이 아니라 고객 AWS 계정에서 만든 IAM Role ARN입니다.

## 14. ASP S3 연결 생성

Unified AWS Access 인증이 끝난 후 ASP Connection Registry에 등록합니다.

1. Atlas 프로젝트에서 `Stream Processing`으로 이동합니다.
2. 사용할 Workspace에서 `Manage`를 클릭합니다.
3. `Connection Registry`를 선택합니다.
4. `Add Connection`을 클릭합니다.
5. Connection Type으로 `S3`를 선택합니다.
6. Connection 이름을 입력합니다.

```text
aws_iceberg_s3
```

7. AWS IAM Role ARN 드롭다운에서 인증한 고객 Role을 선택합니다.

```text
arn:aws:iam::123456789012:role/mongodb-atlas-asp-iceberg-writer
```

8. 연결을 생성합니다.

ASP 파이프라인에서는 다음과 같이 사용합니다.

```javascript
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
```





## 15. S3 Bucket Policy

S3 버킷과 IAM Role이 같은 AWS 계정에 있고 Bucket Policy에 별도 제한이나 명시적 Deny가 없다면 Role의 IAM Permission Policy만으로 접근할 수 있습니다.

다음 환경에서는 Bucket Policy를 추가로 확인해야 합니다.

- S3 버킷과 IAM Role이 서로 다른 AWS 계정에 있음
- 특정 VPC Endpoint만 허용함
- 특정 `aws:PrincipalArn`만 허용함
- AWS Organizations의 Organization ID 조건을 사용함
- 기본적으로 모든 접근을 Deny함
- SSE-KMS를 특정 Principal로 제한함

Bucket Policy의 명시적 `Deny`는 IAM Role의 `Allow`보다 우선합니다.

초기 구축에서는 S3 버킷, Glue Catalog와 IAM Role을 같은 AWS 계정에 두는 구성을 권장합니다.

## 17. Lake Formation

AWS Glue Data Catalog가 Lake Formation으로 관리되는 환경에서는 IAM 권한만으로 충분하지 않을 수 있습니다.

Lake Formation을 사용한다면 다음 권한을 추가로 검토합니다.

```text
DATA_LOCATION_ACCESS
CREATE_TABLE
ALTER
INSERT
DELETE
DESCRIBE
```

실제 필요 권한은 Lake Formation의 등록된 S3 Location, Database/Table 권한 모델과 ASP가 수행하는 작업에 따라 달라집니다.

Lake Formation을 사용하지 않는 일반 Glue Catalog 환경이라면 이 단계는 생략할 수 있습니다.

## 18. 연결 검증

### 18.1 Atlas Role 검증

Atlas의 `Validate and Finish`가 성공해야 합니다.

이 단계에서 실패하면 주로 Trust Policy 문제입니다.

- Atlas Principal ARN 오류
- External ID 오류
- 고객 Role ARN 입력 오류
- `sts:AssumeRole` 누락
- IAM 변경 사항 전파 지연

IAM 변경 직후에는 수십 초 정도 기다렸다가 다시 검증할 수 있습니다.

### 18.2 ASP 쓰기 검증

Role 검증은 AssumeRole 성공 여부를 중심으로 확인합니다. S3와 Glue의 실제 권한은 ASP 프로세서를 실행해서 검증해야 합니다.

정상 처리 시 다음 리소스가 생성됩니다.

```text
S3:
s3://my-company-analytics-dev/iceberg/asp_analytics/...

Glue:
Database = asp_analytics
Table = movies_current
```

### 18.3 CloudTrail 검증

AWS CloudTrail Event History에서 다음 이벤트를 확인합니다.

```text
AssumeRole
PutObject
GetObject
DeleteObject
CreateTable
UpdateTable
```

`AssumeRole`은 성공하지만 `PutObject`가 실패한다면 Trust Policy보다 Permission Policy, Bucket Policy 또는 KMS Policy를 확인해야 합니다.

## 19. 오류 진단

| 오류 | 주요 원인 |
| --- | --- |
| `AccessDenied: sts:AssumeRole` | Trust Policy 또는 External ID 오류 |
| `AccessDenied: s3:GetBucketLocation` | 버킷 ARN 또는 권한 누락 |
| `AccessDenied: s3:ListBucket` | 버킷 ARN 또는 Prefix 조건 오류 |
| `AccessDenied: s3:PutObject` | Object ARN, Bucket Policy 또는 KMS 오류 |
| `AccessDenied: s3:DeleteObject` | Iceberg 파일 정리 권한 누락 |
| `AccessDenied: glue:CreateTable` | Glue Database/Table ARN 또는 Lake Formation 오류 |
| `AccessDenied: glue:UpdateTable` | Iceberg Commit에 필요한 Glue 권한 누락 |
| `AccessDenied: kms:GenerateDataKey` | KMS IAM Policy 또는 Key Policy 오류 |
| `Lake Formation permission denied` | Lake Formation Grant 누락 |
| Atlas Role 검증 실패 | 고객 Role ARN 또는 Trust Policy 오류 |
| S3 연결 성공 후 테이블 생성 실패 | Glue, KMS 또는 Lake Formation 권한 오류 |

## 20. 보안 권장사항

- `AmazonS3FullAccess` 대신 특정 버킷과 Prefix만 허용합니다.
- `AWSGlueConsoleFullAccess` 대신 특정 Glue Database/Table만 허용합니다.
- Trust Policy의 Principal에 `*`를 사용하지 않습니다.
- Atlas가 제공한 External ID 조건을 반드시 사용합니다.
- 개발, 검증, 운영 환경마다 별도의 IAM Role을 만듭니다.
- 가능하면 Atlas 프로젝트별 IAM Role을 분리합니다.
- ASP Writer Role과 Athena Analyst Role을 분리합니다.
- IAM Role과 S3 버킷을 같은 AWS 계정에 둡니다.
- KMS Key도 환경별로 분리합니다.
- AWS CloudTrail로 AssumeRole, S3 및 Glue 작업을 감사합니다.
- Permission Boundary 및 AWS Organizations SCP의 명시적 Deny를 확인합니다.
- IAM Access Analyzer로 외부 계정 Trust를 검토합니다.

## 21. 최종 구성

```text
AWS Account
  S3 Bucket:
    my-company-analytics-dev

  Glue Database:
    asp_analytics

  IAM Permission Policy:
    MongoDBAtlasASPIcebergWriterPolicy

  IAM Role:
    mongodb-atlas-asp-iceberg-writer

  Trust Policy:
    Atlas AWS Account ARN
    + Atlas Project External ID
    + sts:AssumeRole

MongoDB Atlas
  Project Integrations:
    고객 IAM Role ARN 인증

  ASP Connection Registry:
    aws_iceberg_s3

  Stream Processor:
    $iceberg.connectionName = aws_iceberg_s3
```

## 22. 구축 체크리스트

- [ ] S3 버킷과 Prefix 확정
- [ ] Glue Database 사전 생성 여부 결정
- [ ] KMS 암호화 방식 확인
- [ ] Lake Formation 사용 여부 확인
- [ ] Atlas 프로젝트에서 Unified AWS Access 시작
- [ ] Atlas AWS Account ARN 확인
- [ ] Atlas External ID 확인
- [ ] 최소 권한 IAM Permission Policy 생성
- [ ] Custom Trust Policy로 IAM Role 생성
- [ ] IAM Role에 Permission Policy 연결
- [ ] Trust Policy Principal 확인
- [ ] Trust Policy External ID 확인
- [ ] Atlas에 고객 IAM Role ARN 입력
- [ ] `Validate and Finish` 성공 확인
- [ ] ASP Connection Registry에 S3 연결 생성
- [ ] 테스트 Stream Processor 시작
- [ ] S3 Iceberg 파일 생성 확인
- [ ] Glue Table 생성 확인
- [ ] Athena 조회 확인
- [ ] CloudTrail AssumeRole 및 PutObject 확인
- [ ] 운영 환경에서 Full Access 정책 제거 확인

## 23. 공식 문서

- [MongoDB Atlas Unified AWS Access](https://www.mongodb.com/docs/atlas/security/set-up-unified-aws-access/)
- [Atlas Stream Processing Connection](https://www.mongodb.com/docs/atlas/atlas-stream-processing/add-sp-connection/)
- [Atlas Stream Processing `$iceberg`](https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-iceberg/)
- [AWS Third-Party Role and External ID](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)
