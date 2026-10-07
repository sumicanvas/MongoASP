# ASP → Iceberg: Multi-Collection Initial Sync 및 CDC 중복 처리 FAQ

- 기준일: 2026-10-07
- 용도: 고객 문의 대응을 위한 내부 기술 검토
- 검토 근거: MongoDB 공식 문서, 내부 Field FAQ, 엔지니어링 논의, 구현 및 회귀 테스트
- 검증 범위: 문서와 구현을 대조한 결과이며, 고객 배포 환경의 버전과 전체 파이프라인을 직접 검증한 결과는 아닙니다.

## 1. 문의 배경

고객은 개발 환경에서 Atlas Stream Processing(ASP)을 이용해 MongoDB Atlas의 여러 컬렉션을 Apache Iceberg 테이블로 동기화하고 있습니다.

### 1.1 컬렉션 추가 시 체크포인트 유지 불가

하나의 프로세서에 다음과 같이 명시적인 컬렉션 목록과 initialSync를 설정해 초기 적재를 완료했습니다.

```javascript
{
  $source: {
    connectionName: "<atlas-connection>",
    db: "<source-database>",
    coll: ["a", "b"],
    initialSync: { enable: true }
  }
}
```

이후 목록에 다른 컬렉션을 추가하자 저장 단계에서 다음 오류가 발생했습니다.

```text
Failed to modify stream processor:
resumeFromCheckpoint must be false to modify a stream processor's $source stage
```

### 1.2 재적재 후 동일 `_id` 중복

고객은 `mode: "cdc"`, `idFieldName: "_id"`를 사용했지만, 특정 컬렉션을 재적재한 뒤 다음 결과를 관찰했습니다.

| 항목 | 재적재 전 | 재적재 후 |
| --- | ---: | ---: |
| 전체 행 수 | 2,340 | 4,671 |
| 고유 `_id` 수 | 2,340 | 2,340 |

추가로 `$setStreamMeta`를 이용해 operation type을 `update`로 바꾸었을 때는 의도한 upsert 동작을 확인했다고 전달했습니다.

### 1.3 DB 단위 소스의 initialSync 실패

`coll`을 생략해 DB 단위 change stream을 사용하면서 initialSync를 활성화하면 다음 오류가 발생했습니다.

```text
StreamProcessorInvalidOptions: initialSync requires either
a single collection (db + coll) or an explicit collection list (db + coll: [...])
```

## 2. 핵심 결론

고객의 이해는 대체로 맞으며, `$setStreamMeta`로 `insert`를 `update`로 변경해 재적재 시 upsert시키는 방식도 현재 구현과 부합합니다.

| 질문 | 검토 결과 |
| --- | --- |
| 기존 프로세서에 컬렉션을 추가하면서 신규 컬렉션만 initialSync할 수 있는가? | 기존 체크포인트를 유지하면서 `coll` 목록을 확장하는 방법은 현재 지원되는 구성으로 확인되지 않았습니다. |
| `mode: "cdc"`, `idFieldName: "_id"`이면 insert도 자동 upsert되는가? | 아닙니다. operation metadata가 작업을 결정하며, `idFieldName`은 대상 행을 식별하는 키입니다. |
| `$setStreamMeta`로 insert를 update로 바꾸는 방식이 타당한가? | 현재 구현 및 회귀 테스트와 부합합니다. delete 처리와 기존 source metadata를 보존해야 합니다. |
| 이미 생긴 중복도 update로 제거되는가? | 아닙니다. 동일 키의 여러 행을 모두 갱신하면서 행 수가 유지될 수 있습니다. |
| DB 단위 initialSync와 확정된 지원 계획이 있는가? | 현재 구현은 단일 컬렉션 또는 명시적 목록을 요구합니다. 확정된 지원 일정은 확인하지 못했습니다. |
| 권장 구성은 무엇인가? | 독립적으로 추가·재동기화해야 하는 컬렉션 또는 고정된 컬렉션 그룹 단위로 프로세서를 분리하는 구성을 권장합니다. |

> 핵심 구분: **재적재 시 중복 행 생성을 예방하는 것**과 **이미 생성된 중복 행을 제거하는 것**은 서로 다른 작업입니다.

## 3. 질문 1: 컬렉션 추가 시 신규 컬렉션만 initialSync할 수 있나요?

### 3.1 완료된 컬렉션을 건너뛰는 조건

Multi-Collection Initial Sync 문서의 “완료된 컬렉션은 건너뛴다”는 설명은 **기존 initialSync의 진행 상태를 체크포인트에서 복구하는 경우**를 의미합니다.

| 상황 | 동작 |
| --- | --- |
| 같은 `$source` 구성으로 체크포인트에서 재개 | 완료된 컬렉션을 건너뛰고, 진행 중이던 작업을 이어서 수행합니다. |
| `coll: ["a", "b"]`를 `["a", "b", "c"]`로 변경 | `$source` 변경이므로 `resumeFromCheckpoint: false`가 필요합니다. |
| 체크포인트를 버리고 initialSync를 활성화한 상태로 시작 | 이전 완료 상태를 이용하지 못하므로 기존 컬렉션도 다시 복사합니다. |

공식 프로세서 수정 문서에도 `resumeFromCheckpoint: true`일 때는 `$source`를 수정할 수 없다고 명시되어 있습니다. 따라서 고객이 본 오류와 문서의 skip 설명은 서로 모순되지 않습니다.

체크포인트를 버리는 것은 프로세서의 재개 상태를 초기화하는 작업입니다. 기존 Iceberg 테이블의 데이터를 자동으로 비우는 작업은 아니므로, 재적재 시 sink의 쓰기 방식이 중요합니다.

출처: [프로세서 수정 제한][public-manage], [Multi-Collection Initial Sync][public-multi-sync], [ASP 체크포인트 아키텍처][public-architecture]

### 3.2 현재 요구사항에 맞는 구성

기존 프로세서의 `coll` 목록을 확장하면서 체크포인트를 유지하고 추가된 컬렉션만 initialSync하는 방법은 현재 지원되는 구성으로 확인되지 않았습니다.

가장 직접적인 구성은 다음과 같습니다.

- 기존 `a`, `b` 프로세서는 체크포인트를 유지하며 계속 실행합니다.
- 신규 `c`를 담당하는 프로세서를 추가합니다.
- 신규 프로세서가 `c`의 initialSync를 수행하고, 이후 CDC도 계속 담당합니다.

## 4. 질문 2: CDC 모드인데 왜 중복되며, `$setStreamMeta` 방식은 올바른가요?

### 4.1 `operationType`과 `idFieldName`의 역할

- **`operationType`**: append, upsert, delete 중 어떤 작업을 할지 결정합니다.
- **`idFieldName`**: upsert/delete 시 어떤 키로 대상 행을 찾을지 결정합니다.
- **`idFieldName`은 unique constraint를 생성하거나 모든 insert를 upsert로 바꾸는 옵션이 아닙니다.**

현재 Iceberg CDC 구현은 다음과 같이 동작합니다.

| `stream.source.operationType` | Iceberg 동작 |
| --- | --- |
| `insert` | 새 행을 추가합니다. 같은 키가 존재하더라도 중복 행이 생길 수 있습니다. |
| `update` / `replace` | `idFieldName` 기준으로 행을 교체하고, 없으면 삽입하는 upsert를 수행합니다. |
| `delete` | 해당 키의 행을 삭제합니다. |
| 그 외 값 또는 메타데이터 누락 | 현재 구현에서는 insert로 처리합니다. |

공식 문서는 CDC 모드가 `stream.source.operationType` 메타데이터를 읽는다고 설명합니다. 내부 구현의 `IcebergWriter::getOpType()`에서는 `update`와 `replace`를 동일한 replace 경로로 매핑합니다.

또한 `$iceberg`의 `mode: "insert"`는 operation metadata를 무시하고 모든 입력을 append합니다. `mode: "cdc"`에서도 개별 이벤트의 operation type이 `insert`라면 append 경로를 사용한다는 점을 구분해야 합니다.

출처: [공식 `$iceberg` 문서][public-iceberg], [내부 Iceberg operator 구현][internal-iceberg-operator]

### 4.2 initialSync의 insert 이벤트 처리

initialSync는 기존 문서를 insert change event처럼 내보냅니다. 이를 Iceberg가 append하는 것은 현재 동작과 일치합니다.

따라서 고객이 관찰한 “전체 행 수는 증가하지만 고유 `_id` 수는 그대로”인 결과는 이 동작으로 설명할 수 있습니다. 다만 정확한 증가 건수와 전체 데이터 정합성은 고객의 파이프라인, 이벤트 처리 이력 및 대상 테이블 조회 결과를 통해 확인해야 합니다.

출처: [Multi-Collection Initial Sync][public-multi-sync], [내부 Iceberg operator 구현][internal-iceberg-operator]

### 4.3 `$setStreamMeta`를 이용한 insert → update 변환

**현재 상태를 미러링하고 재전달·재적재에 따른 append를 피하려는 목적이라면 타당한 방식입니다.**

내부에는 `$setStreamMeta`로 operation type을 `replace`로 설정한 뒤 동일 키가 반복 출력되어도 upsert되는지를 검증하는 회귀 테스트가 있습니다. `update`와 `replace`는 현재 Iceberg 구현에서 같은 처리 경로를 사용합니다.

출처: [내부 Iceberg `$setStreamMeta` 회귀 테스트][internal-set-meta-test], [내부 Iceberg operator 구현][internal-iceberg-operator]

모든 이벤트를 무조건 update로 바꾸기보다 **`insert`만 변경하고 `delete` 등은 유지**하는 것이 좋습니다. 다음은 기존 `$match`와 `$replaceRoot` 뒤, `$iceberg` 직전에 추가할 수 있는 예시입니다.

```javascript
{
  $setStreamMeta: {
    "stream.source": {
      $mergeObjects: [
        { $meta: "stream.source" },
        {
          operationType: {
            $cond: [
              {
                $eq: [
                  { $meta: "stream.source.operationType" },
                  "insert"
                ]
              },
              "update",
              { $meta: "stream.source.operationType" }
            ]
          }
        }
      ]
    }
  }
}
```

이 예시의 적용 조건과 의미는 다음과 같습니다.

- initialSync와 일반 CDC의 `insert`를 upsert 경로로 보냅니다.
- `delete`는 그대로 유지합니다.
- `stream.source.ns` 등 기존 메타데이터를 보존하므로 동적 테이블 라우팅에 필요한 정보도 유지합니다.
- `$set`으로 **문서 본문의** `operationType`만 변경하는 것과 다릅니다. Iceberg가 읽는 것은 **메타데이터**입니다.
- `update`도 여기서는 행 교체 의미이므로, sink에는 변경분만이 아니라 **전체 문서와 안정적인 `_id`**가 전달되어야 합니다.
- change event의 최상위 `_id`는 resume token입니다. `$replaceRoot` 이후 sink에 전달하는 `_id`가 실제 원본 문서의 키인지 확인해야 합니다. delete는 `documentKey`를 사용합니다.
- 공식 예시의 `fullDocument: "required"`를 사용한다면 대상 소스 컬렉션들에 pre-/post-images 설정이 필요합니다.

내부 구문 검증에서는 `$setStreamMeta`의 키에 `stream.` 이후 추가 점을 허용하지 않습니다. 위 예시는 `"stream.source.operationType"`을 직접 설정하는 대신 `"stream.source"` 객체를 병합하는 형태입니다.

출처: [공식 `$setStreamMeta` 문서][public-set-meta], [내부 `$setStreamMeta` 구문 검증 테스트][internal-set-meta-validation], [공식 `$iceberg` 미러링 예시][public-iceberg]

이 코드는 적용 예시이며, 고객의 실제 파이프라인과 배포 환경에서 검증해야 합니다. 현재 구현과 회귀 테스트가 이 방식을 뒷받침하지만, 이것만으로 고객 환경의 모든 재시작·재적재 시나리오에 대한 지원 보장이 확인된 것은 아닙니다.

### 4.4 이미 생긴 중복은 자동으로 제거되지 않음

내부 테스트에는 다음 동작이 명시되어 있습니다.

```text
동일 키로 A, B, C 세 행을 insert
→ 같은 키에 값 D로 update
→ 결과는 D 한 행이 아니라 D, D, D 세 행
```

즉, **기존 중복 행을 모두 갱신하지만 행 수는 유지할 수 있습니다.** `replace` 역시 같은 처리 경로이므로 이름만 바꾼다고 중복 정리가 되지는 않습니다.

출처: [내부 `iceberg_duplicate_rows.js`][internal-duplicate-test]

2026년 9월 22일 엔지니어링 논의에서도 다음이 확인됩니다.

- 현재 컬럼에 unique-key constraint를 적용하지 않습니다.
- 동일 키의 insert 두 건은 compaction 후에도 유지됩니다.
- update가 동일 키의 여러 행을 같은 내용으로 갱신할 수 있습니다.

출처: [내부 Iceberg 엔지니어링 논의][internal-iceberg-discussion]

따라서 이미 중복된 테스트 테이블은 **별도 정리 또는 새 테이블로 재구축한 뒤**, insert → update 변환을 적용한 상태에서 재적재 테스트를 수행하는 것이 좋습니다.

**Compaction을 기다리면 중복이 없어진다고 안내해서는 안 됩니다.** ASP의 Iceberg 출력 보장은 여전히 **at-least-once**이며, 이 변환이 exactly-once 보장으로 바뀌는 것은 아닙니다.

출처: [공식 `$iceberg` 처리 보장][public-iceberg]

## 5. 질문 3: DB 단위 소스의 initialSync 또는 향후 계획이 있나요?

### 5.1 DB 단위 change stream과 initialSync의 차이

`coll`을 생략한 DB 단위 change stream과 DB 전체의 기존 데이터를 initialSync하는 기능은 구분해야 합니다.

현재 내부 소스 구현은 initialSync를 활성화할 때 다음 중 하나를 명시적으로 요구합니다.

- `db + coll: "collection"`
- `db + coll: ["a", "b", ...]`

그렇지 않으면 고객이 받은 것과 동일한 오류를 발생시킵니다.

출처: [공식 `$source` 문서][public-source], [내부 initialSync 옵션 검증][internal-source-operator]

### 5.2 초기 적재를 반드시 ASP 외부에서 해야 하는 것은 아님

외부 스크립트 등으로 현재 컬렉션 목록만 구한 뒤, 이를 명시적인 `coll` 배열 또는 여러 프로세서에 넣으면 **실제 초기 적재는 ASP에서 수행할 수 있습니다.** 이 목록이 이후 자동 확장되는 것은 아닙니다.

DB 단위 CDC를 유지하면서 별도 초기 적재 경로를 조합하는 설계도 가능하지만, 초기 적재와 CDC의 연결 시점, 이벤트 순서, 누락 방지를 별도로 설계해야 합니다.

### 5.3 향후 계획

DB 전체 initialSync 및 기존 프로세서에 컬렉션을 점진적으로 추가하는 기능의 **확정된 지원 일정은 이번에 확인한 자료에서는 찾지 못했습니다.**

고객에게 일정이나 지원 예정 여부를 확약하기보다는 제품팀에 확인할 항목입니다. 내부 코드의 존재나 master 브랜치 반영만으로 고객 환경의 배포 여부를 판단해서는 안 됩니다.

## 6. 질문 4: 컬렉션별 프로세서 분리가 권장 패턴인가요?

### 6.1 이번 요구사항에 대한 권고

이번 요구사항에는 **독립적으로 추가·재동기화해야 하는 단위별 프로세서 분리**를 추천합니다. 반드시 컬렉션 하나당 프로세서 하나로 고정할 필요는 없습니다.

```text
프로세서 P1: coll: ["a", "b"] → Iceberg table_a, table_b
프로세서 P2: coll: ["c"]      → Iceberg table_c
```

- 기존 P1은 체크포인트를 유지하며 계속 실행합니다.
- 신규 P2만 초기 적재하고 이후 CDC를 이어갑니다.
- 함께 운영할 고정된 컬렉션들은 하나의 프로세서로 묶을 수 있습니다.
- 개별 재적재, 장애 격리, 다른 변환 로직이 필요한 컬렉션은 분리합니다.

이는 이번 운영 요구사항에 대한 설계 권고입니다. 공식 문서에도 multi-collection → multi-table Iceberg 구성이 있으므로, multi-collection 자체를 피해야 하는 것은 아닙니다.

한 프로세서로 묶으면 컬렉션 하나의 initialSync 실패가 전체 프로세서 실패로 이어질 수 있다는 점도 고려해야 합니다.

출처: [공식 `$iceberg` 동적 라우팅 예시][public-iceberg], [Multi-Collection Initial Sync 동작 특성][public-multi-sync]

### 6.2 비용과 테이블 한도

기준일 현재 `$iceberg`의 지원 tier 및 동적 라우팅 테이블 한도는 다음과 같습니다.

| Processor tier | 최대 동적 라우팅 테이블 수 |
| --- | ---: |
| SP10 | 5 |
| SP30 | 10 |
| SP50 | 50 |

프로세서는 실행 중 각자의 리소스로 과금되므로 작은 컬렉션을 모두 개별 프로세서로 나누면 비용이 커질 수 있습니다. 소스 initialSync 컬렉션 수 제한도 별도로 확인해야 합니다.

출처: [공식 `$iceberg` 지원 tier 및 테이블 한도][public-iceberg], [프로세서 리소스·과금 구조][public-architecture], [내부 Field FAQ][internal-field-faq]

## 7. Support 확인 사항

### 7.1 `SERVER-125082` 적용 여부

`$setStreamMeta`로 설정한 중첩 operation metadata가 후속 연산에서 소실되는 버그의 수정 이력이 있습니다.

현재 구현과 회귀 테스트는 확인되지만, 고객 배포 환경의 수정 포함 여부까지 확인한 것은 아닙니다. Support를 통해 해당 환경의 수정 포함 여부를 확인하는 것이 좋습니다.

출처: [내부 `SERVER-125082` 수정 PR][internal-set-meta-fix]

### 7.2 고객의 upsert 테스트 조건

다음 두 경우를 구분해야 합니다.

1. 중복 없는 테이블에서 재적재한 뒤 행 수가 유지되는지
2. 이미 중복된 테이블에 update를 적용한 뒤 행 수가 감소하는지

두 번째 결과는 첫 번째 결과로 보장되지 않습니다. 다음 자료를 확보하면 분석에 도움이 됩니다.

- 소스 클러스터, ASP workspace 및 processor 식별 정보
- 테스트 수행 시간대와 타임존
- 변경 전후 전체 파이프라인 및 `$setStreamMeta` 설정
- 테스트 전후 대상 테이블 상태와 `_id`별 행 수
- 최신 Iceberg 스냅샷을 조회한 집계 SQL 및 조회 엔진
- initialSync 진행 상태, 프로세서 통계 및 관련 DLQ 내용

### 7.3 체크포인트 초기화 후 삭제 정합성

새 initialSync는 현재 존재하는 문서를 복사합니다. 따라서 이전 CDC에서 놓쳤고 새 initialSync 시작 전에 이미 삭제된 문서는, upsert 재적재만으로 대상에서 제거되지 않을 수 있습니다.

이는 initialSync가 현재 문서를 복사하고 해당 동기화의 시작 위치부터 변경 이벤트를 재생한다는 구조에서 도출되는 운영상 고려사항입니다. 재동기화를 전체 정합성 복구 절차로 사용할 경우, 기존 대상에만 남은 행을 어떻게 처리할지도 확인해야 합니다.

출처: [ASP initialSync 및 체크포인트 아키텍처][public-architecture]

## 8. 고객에게 전달할 핵심 메시지

> 현재 동작에 대한 이해는 대체로 맞습니다. Iceberg CDC는 operation type으로 쓰기 방식을 결정하고, `idFieldName`으로 갱신·삭제할 행을 식별합니다. initialSync의 insert 이벤트는 append될 수 있으며, 이를 `$setStreamMeta`로 update로 변경하는 방식은 현재 구현과 부합합니다. 다만 이미 생성된 중복 행까지 자동으로 제거하는 것은 아니므로 기존 중복은 별도 정리가 필요합니다. 컬렉션을 추가할 때는 기존 프로세서의 체크포인트를 유지하고, 신규 컬렉션 또는 신규 고정 그룹을 별도 프로세서로 추가하는 구성을 권장합니다. DB 단위 initialSync 및 기존 목록의 점진적 확장에 대한 확정 일정은 제품팀 확인이 필요합니다.

## 9. 참고 자료

### 9.1 공개 문서

- [Configure Multi-Collection Initial Sync][public-multi-sync]
- [Develop and Manage Stream Processors][public-manage]
- [Atlas Stream Processing Architecture][public-architecture]
- [`$source` Stage][public-source]
- [`$iceberg` Aggregation Stage][public-iceberg]
- [`$setStreamMeta` Aggregation Stage][public-set-meta]

### 9.2 내부 기술 근거

- [Field FAQ: ASP Iceberg Support - AWS][internal-field-faq]: 제품 동작, 지원 tier 및 과금 관련 내부 설명
- [Iceberg operator 구현][internal-iceberg-operator]: `IcebergWriter::getOpType()`의 operation type 분기
- [Iceberg `$setStreamMeta` 회귀 테스트][internal-set-meta-test]: metadata 변경에 따른 upsert 검증
- [`$setStreamMeta` 구문 검증 테스트][internal-set-meta-validation]: metadata key의 점 표기 제한
- [Iceberg 중복 행 테스트][internal-duplicate-test]: 동일 키의 중복 행을 update해도 행 수가 유지되는 동작
- [Iceberg 엔지니어링 논의, 2026-09-22][internal-iceberg-discussion]: unique constraint 부재, compaction과 중복, update 처리 확인
- [Change stream source 구현][internal-source-operator]: initialSync의 단일 컬렉션·명시적 목록 요구 검증
- [`SERVER-125082` 수정 PR][internal-set-meta-fix]: 중첩 source metadata 소실 관련 수정

내부 Field FAQ는 일반적인 제품 설명을 제공하며, 이번 문의의 세부 처리 의미는 엔지니어링 논의와 구현·회귀 테스트가 더 직접적인 근거입니다. 내부 자료 링크는 내부 검토용입니다.

[public-multi-sync]: https://www.mongodb.com/docs/atlas/atlas-stream-processing/multi-collection-initial-sync/
[public-manage]: https://www.mongodb.com/docs/atlas/atlas-stream-processing/manage-stream-processor/
[public-architecture]: https://www.mongodb.com/docs/atlas/atlas-stream-processing/architecture/
[public-source]: https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-source/
[public-iceberg]: https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-iceberg/
[public-set-meta]: https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-setStreamMeta/
[internal-field-faq]: https://docs.google.com/document/d/12VSxCknOHAernl4tGgfvw8ENVMW4Ij5TA5-iwKHcZx8
[internal-iceberg-operator]: https://github.com/10gen/mongo/blob/master/src/mongo/db/modules/enterprise/src/streams/exec/iceberg/iceberg_operator.cpp
[internal-set-meta-test]: https://github.com/10gen/mongo/blob/master/src/mongo/db/modules/enterprise/jstests/streams/aspio/iceberg/iceberg_setstreammeta.js
[internal-set-meta-validation]: https://github.com/10gen/mongo/blob/master/src/mongo/db/modules/enterprise/jstests/streams/set_stream_meta_error.js
[internal-duplicate-test]: https://github.com/10gen/mongo/blob/master/src/mongo/db/modules/enterprise/jstests/streams/aspio/iceberg/iceberg_duplicate_rows.js
[internal-iceberg-discussion]: https://mongodb.slack.com/archives/C08LRG8UFU4/p1790095077189689
[internal-source-operator]: https://github.com/10gen/mongo/blob/master/src/mongo/db/modules/enterprise/src/streams/exec/change_stream_source_operator.cpp
[internal-set-meta-fix]: https://github.com/10gen/mongo/pull/53523
