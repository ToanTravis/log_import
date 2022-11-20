# a1-log-import
ログ取り込み用のコンテナ、ECSタスク及び起動用のLambda関数を管理する。
GitHub Actionsからデプロイ、手動実行が可能。

## ログ種別
### YOne
- Dsp Imp
- Network Imp
- App Incoming Request
- App Outgoing Request
### FOneH
- Imp
- Imp Decision
- Click
- Vast Event
- Adslot View

### BidCore
- bid_request_app

## ログファイルの配置
S3に配置
- 開発環境
  - ログファイル: `s3://audienceone-development/data/aone_odessa_dev/{platform}_{log_type}/yyyy-mm-dd/{file}`
  - メタデータファイル: `s3://audienceone-development/data/aone_odessa_dev/metadata/{platform}_{log_type}`

- 本番環境
  - ログファイル: `s3://audienceone/data/{platform}/{log_type}/yyyy-mm-dd/` 
  - メタデータファイル: `s3://audienceone/data/{platform}/metadata/{log_type}`

## ログファイルの状態
ログファイルはファイル名のprefixで以下のように状態を管理している
- 未取り込み: prefixなし
  - ex.: `2022-06-10-16...log.gz`
- 取り込み中: `__`(アンダースコア2つ)
  - ex.: `__2022-06-10-16...log.gz`
- 取り込み済み: `_`(アンダースコア1つ)
  - ex.: `_2022-06-10-16...log.gz`

## デプロイ
GitHub Actionsでビルド、samコマンドでCloud formationテンプレートを利用してデプロイ
- 開発環境

    GitHub Actionsの[【Development】Deploy ECR, ECS, SAM template](https://github.com/DAConsortium/a1-log-import/actions/workflows/deploy-development.yaml)でブランチを選択して実行

- 本番環境

    `production`ブランチにマージするとGithub Actionsの[【Production】Deploy ECR, ECS, SAM template](https://github.com/DAConsortium/a1-log-import/actions/workflows/deploy-production.yaml)が実行される

## 手動実行
- 開発環境
GitHub Actionsの[【Development】Execute A1 Log Import Manually](https://github.com/DAConsortium/a1-log-import/actions/workflows/log-import-execute-manually-development.yaml)から実行

- 本番環境
GitHub Actionsの[【Production】Execute A1 Log Import Manually](https://github.com/DAConsortium/a1-log-import/actions/workflows/log-import-execute-manually-production.yaml)から実行

- パラメーター
    - Log platform (必須)
    - Log type (必須)
    - Version of metadata of log import
    - Input start date 【YYYY-MM-DD】. Default is yesterday.
    - Input end date 【YYYY-MM-DD】. Default is today.
    - Maximum number of files to be imported in one task. Default is 200

## アーキテクチャ
![ログ取り込みアーキテクチャ](https://user-images.githubusercontent.com/39155017/173006848-5a80e2d8-d798-4ef5-8488-0b06c49a18fe.png)