from protos.base_pb2 import Source, SourceKeyType, SourceModelType

integrations_connector_type_connector_keys_map = {
    Source.PAGER_DUTY: [
        [
            SourceKeyType.PAGER_DUTY_API_KEY,
            SourceKeyType.PAGER_DUTY_CONFIGURED_EMAIL
        ]
    ],
    Source.ROOTLY: [
        [
            SourceKeyType.ROOTLY_API_KEY,
        ]
    ],
    Source.ZENDUTY: [
        [
            SourceKeyType.ZENDUTY_API_KEY,
        ]
    ],
    Source.SLACK: [
        [
            SourceKeyType.SLACK_BOT_AUTH_TOKEN,
            SourceKeyType.SLACK_APP_ID
        ]
    ],
    Source.NEW_RELIC: [
        [
            SourceKeyType.NEWRELIC_API_KEY,
            SourceKeyType.NEWRELIC_APP_ID,
            SourceKeyType.NEWRELIC_API_DOMAIN
        ]
    ],
    Source.DATADOG: [
        [
            SourceKeyType.DATADOG_API_KEY,
            SourceKeyType.DATADOG_APP_KEY,
            SourceKeyType.DATADOG_API_DOMAIN
        ]
    ],
    Source.DATADOG_OAUTH: [
        [
            SourceKeyType.DATADOG_AUTH_TOKEN,
            SourceKeyType.DATADOG_API_DOMAIN,
            SourceKeyType.DATADOG_API_KEY
        ]
    ],
    Source.GRAFANA: [
        [
            SourceKeyType.GRAFANA_HOST,
            SourceKeyType.GRAFANA_API_KEY,
            SourceKeyType.SSL_VERIFY,
        ],
        [
            SourceKeyType.GRAFANA_HOST,
            SourceKeyType.GRAFANA_API_KEY
        ]
    ],
    Source.GRAFANA_VPC: [
        [
            SourceKeyType.AGENT_PROXY_HOST,
            SourceKeyType.AGENT_PROXY_API_KEY
        ]
    ],
    Source.AGENT_PROXY: [
        [
            SourceKeyType.AGENT_PROXY_HOST,
            SourceKeyType.AGENT_PROXY_API_KEY
        ]
    ],
    Source.CLOUDWATCH: [
        [
            SourceKeyType.AWS_ACCESS_KEY,
            SourceKeyType.AWS_SECRET_KEY,
            SourceKeyType.AWS_REGION,
        ],
        [
            SourceKeyType.AWS_ASSUMED_ROLE_ARN,
            SourceKeyType.AWS_REGION,
        ]
    ],
    Source.CLICKHOUSE: [
        [
            SourceKeyType.CLICKHOUSE_INTERFACE,
            SourceKeyType.CLICKHOUSE_HOST,
            SourceKeyType.CLICKHOUSE_PORT,
            SourceKeyType.CLICKHOUSE_USER,
            SourceKeyType.CLICKHOUSE_PASSWORD
        ]
    ],
    Source.POSTGRES: [
        [
            SourceKeyType.POSTGRES_HOST,
            SourceKeyType.POSTGRES_USER,
            SourceKeyType.POSTGRES_PASSWORD,
            SourceKeyType.POSTGRES_PORT,
            SourceKeyType.POSTGRES_DATABASE
        ]
    ],
    Source.EKS: [
        [
            SourceKeyType.AWS_ACCESS_KEY,
            SourceKeyType.AWS_SECRET_KEY,
            SourceKeyType.AWS_REGION,
            SourceKeyType.EKS_ROLE_ARN,
        ],
        [
            SourceKeyType.AWS_REGION,
            SourceKeyType.AWS_ASSUMED_ROLE_ARN,
            SourceKeyType.EKS_ROLE_ARN,
        ]
    ],
    Source.SQL_DATABASE_CONNECTION: [
        [
            SourceKeyType.SQL_DATABASE_CONNECTION_STRING_URI,
        ]
    ],
    Source.OPEN_AI: [
        [
            SourceKeyType.OPEN_AI_API_KEY,
        ]
    ],
    Source.GRAFANA_MIMIR: [
        [
            SourceKeyType.MIMIR_HOST,
            SourceKeyType.X_SCOPE_ORG_ID,
            SourceKeyType.SSL_VERIFY,
        ],
        [
            SourceKeyType.MIMIR_HOST,
            SourceKeyType.X_SCOPE_ORG_ID
        ]
    ],
    Source.BASH: [
        [
            SourceKeyType.REMOTE_SERVER_USER,
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PEM,
            SourceKeyType.REMOTE_SERVER_PASSWORD,
            SourceKeyType.REMOTE_SERVER_PORT,
        ],
        [
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PEM,
            SourceKeyType.REMOTE_SERVER_PASSWORD,
            SourceKeyType.REMOTE_SERVER_PORT,
        ],
        [
            SourceKeyType.REMOTE_SERVER_USER,
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PEM,
            SourceKeyType.REMOTE_SERVER_PASSWORD,
        ],
        [
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PEM,
            SourceKeyType.REMOTE_SERVER_PASSWORD,
        ],
        [
            SourceKeyType.REMOTE_SERVER_USER,
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PEM,
            SourceKeyType.REMOTE_SERVER_PORT,
        ],
        [
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PEM,
            SourceKeyType.REMOTE_SERVER_PORT,
        ],
        [
            SourceKeyType.REMOTE_SERVER_USER,
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PEM
        ],
        [
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PEM
        ],
        [
            SourceKeyType.REMOTE_SERVER_USER,
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PASSWORD,
            SourceKeyType.REMOTE_SERVER_PORT,

        ],
        [
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PASSWORD,
            SourceKeyType.REMOTE_SERVER_PORT,

        ],
        [
            SourceKeyType.REMOTE_SERVER_USER,
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PASSWORD
        ],
        [
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PASSWORD
        ],
        [
            SourceKeyType.REMOTE_SERVER_USER,
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PORT,
        ],
        [
            SourceKeyType.REMOTE_SERVER_HOST,
            SourceKeyType.REMOTE_SERVER_PORT,
        ],
        [
            SourceKeyType.REMOTE_SERVER_USER,
            SourceKeyType.REMOTE_SERVER_HOST,
        ],
        [
            SourceKeyType.REMOTE_SERVER_HOST,
        ],
        [
            SourceKeyType.REMOTE_SERVER_PORT,

        ],
        []
    ],
    Source.AZURE: [
        [
            SourceKeyType.AZURE_CLIENT_ID,
            SourceKeyType.AZURE_CLIENT_SECRET,
            SourceKeyType.AZURE_TENANT_ID,
            SourceKeyType.AZURE_SUBSCRIPTION_ID,
        ]
    ],
    Source.GKE: [
        [
            SourceKeyType.GKE_PROJECT_ID,
            SourceKeyType.GKE_SERVICE_ACCOUNT_JSON,
        ]
    ],
    Source.MS_TEAMS: [
        [
            SourceKeyType.MS_TEAMS_CONNECTOR_WEBHOOK_URL,
        ]
    ],
    Source.ELASTIC_SEARCH: [
        [
            SourceKeyType.ELASTIC_SEARCH_PROTOCOL,
            SourceKeyType.ELASTIC_SEARCH_HOST,
            SourceKeyType.ELASTIC_SEARCH_PORT,
            SourceKeyType.ELASTIC_SEARCH_API_KEY_ID,
            SourceKeyType.ELASTIC_SEARCH_API_KEY,
            SourceKeyType.SSL_VERIFY,
        ],
        [
            SourceKeyType.ELASTIC_SEARCH_PROTOCOL,
            SourceKeyType.ELASTIC_SEARCH_HOST,
            SourceKeyType.ELASTIC_SEARCH_API_KEY_ID,
            SourceKeyType.ELASTIC_SEARCH_API_KEY,
        ],
        [
            SourceKeyType.ELASTIC_SEARCH_HOST,
        ]
    ],
    Source.GRAFANA_LOKI: [
        [
            SourceKeyType.GRAFANA_LOKI_PROTOCOL,
            SourceKeyType.GRAFANA_LOKI_HOST,
            SourceKeyType.GRAFANA_LOKI_PORT,
            SourceKeyType.X_SCOPE_ORG_ID,
            SourceKeyType.SSL_VERIFY
        ],
        [
            SourceKeyType.GRAFANA_LOKI_PROTOCOL,
            SourceKeyType.GRAFANA_LOKI_HOST,
            SourceKeyType.GRAFANA_LOKI_PORT,
            SourceKeyType.X_SCOPE_ORG_ID
        ]
    ],
    Source.KUBERNETES: [
        [
            SourceKeyType.KUBERNETES_CLUSTER_NAME,
            SourceKeyType.KUBERNETES_CLUSTER_API_SERVER,
            SourceKeyType.KUBERNETES_CLUSTER_TOKEN,
            SourceKeyType.KUBERNETES_CLUSTER_CERTIFICATE_AUTHORITY_DATA,
            SourceKeyType.KUBERNETES_CLUSTER_CERTIFICATE_AUTHORITY_PATH
        ],
        [
            SourceKeyType.KUBERNETES_CLUSTER_NAME,
            SourceKeyType.KUBERNETES_CLUSTER_API_SERVER,
            SourceKeyType.KUBERNETES_CLUSTER_TOKEN,
            SourceKeyType.KUBERNETES_CLUSTER_CERTIFICATE_AUTHORITY_DATA
        ],
        [
            SourceKeyType.KUBERNETES_CLUSTER_NAME,
            SourceKeyType.KUBERNETES_CLUSTER_API_SERVER,
            SourceKeyType.KUBERNETES_CLUSTER_TOKEN,
            SourceKeyType.KUBERNETES_CLUSTER_CERTIFICATE_AUTHORITY_PATH
        ],
        [
            SourceKeyType.KUBERNETES_CLUSTER_NAME,
            SourceKeyType.KUBERNETES_CLUSTER_API_SERVER,
            SourceKeyType.KUBERNETES_CLUSTER_TOKEN,
        ],
        []
    ],
    Source.GCM: [
        [
            SourceKeyType.GCM_PROJECT_ID,
            SourceKeyType.GCM_PRIVATE_KEY,
        ]
    ],
    Source.SMTP: [
        [
            SourceKeyType.SMTP_HOST,
            SourceKeyType.SMTP_PORT,
            SourceKeyType.SMTP_USER,
            SourceKeyType.SMTP_PASSWORD,
        ]
    ],
    Source.BIG_QUERY: [
        [
            SourceKeyType.BIG_QUERY_PROJECT_ID,
            SourceKeyType.BIG_QUERY_SERVICE_ACCOUNT_JSON,
        ]
    ],
    Source.MONGODB: [
        [
            SourceKeyType.MONGODB_CONNECTION_STRING,
        ]
    ],
    Source.OPEN_SEARCH: [
        [
            SourceKeyType.OPEN_SEARCH_PROTOCOL,
            SourceKeyType.OPEN_SEARCH_HOST,
            SourceKeyType.OPEN_SEARCH_PORT,
            SourceKeyType.OPEN_SEARCH_USERNAME,
            SourceKeyType.OPEN_SEARCH_PASSWORD,
            SourceKeyType.SSL_VERIFY,
        ],
        [
            SourceKeyType.OPEN_SEARCH_PROTOCOL,
            SourceKeyType.OPEN_SEARCH_HOST,
            SourceKeyType.OPEN_SEARCH_USERNAME,
            SourceKeyType.OPEN_SEARCH_PASSWORD,
            SourceKeyType.SSL_VERIFY,
        ],
        [
            SourceKeyType.OPEN_SEARCH_PROTOCOL,
            SourceKeyType.OPEN_SEARCH_HOST,
            SourceKeyType.OPEN_SEARCH_PORT,
            SourceKeyType.OPEN_SEARCH_USERNAME,
            SourceKeyType.OPEN_SEARCH_PASSWORD,
        ],
        [
            SourceKeyType.OPEN_SEARCH_PROTOCOL,
            SourceKeyType.OPEN_SEARCH_HOST,
            SourceKeyType.OPEN_SEARCH_USERNAME,
            SourceKeyType.OPEN_SEARCH_PASSWORD,
        ],
        [
            SourceKeyType.OPEN_SEARCH_HOST,
        ]
    ],
    Source.GITHUB: [
        [
            SourceKeyType.GITHUB_TOKEN,
            SourceKeyType.GITHUB_ORG,
        ]
    ],
    Source.JENKINS: [
        [
            SourceKeyType.JENKINS_URL,
            SourceKeyType.JENKINS_USERNAME,
            SourceKeyType.JENKINS_API_TOKEN,
            SourceKeyType.JENKINS_CRUMB
        ],
        [
            SourceKeyType.JENKINS_URL,
            SourceKeyType.JENKINS_USERNAME,
            SourceKeyType.JENKINS_API_TOKEN
        ],
    ],
}
integrations_connector_type_display_name_map = {
    Source.SLACK: 'SLACK',
    Source.GOOGLE_CHAT: 'Google Chat',
    Source.SENTRY: 'SENTRY',
    Source.NEW_RELIC: 'NEW RELIC',
    Source.DATADOG: 'DATADOG',
    Source.DATADOG_OAUTH: 'DATADOG',
    Source.GRAFANA: 'GRAFANA',
    Source.GRAFANA_VPC: 'GRAFANA VPC',
    Source.GITHUB: 'GITHUB',
    Source.ELASTIC_APM: 'ELASTIC APM',
    Source.VICTORIA_METRICS: 'VictoriaMetrics',
    Source.PROMETHEUS: 'PROMETHEUS',
    Source.CLOUDWATCH: 'CLOUDWATCH',
    Source.GCM: 'GOOGLE CLOUD MONITORING',
    Source.CLICKHOUSE: 'CLICKHOUSE',
    Source.POSTGRES: 'POSTGRES',
    Source.PAGER_DUTY: 'PAGERDUTY',
    Source.ROOTLY: 'ROOTLY',
    Source.OPS_GENIE: 'OPS GENIE',
    Source.EKS: 'EKS KUBERNETES',
    Source.SQL_DATABASE_CONNECTION: 'SQL DATABASE CONNECTION',
    Source.OPEN_AI: 'OPEN AI',
    Source.BASH: 'BASH COMMAND EXECUTOR',
    Source.GRAFANA_MIMIR: 'GRAFANA MIMIR',
    Source.AZURE: 'AZURE',
    Source.GKE: 'GKE KUBERNETES',
    Source.MS_TEAMS: 'MS TEAMS',
    Source.ELASTIC_SEARCH: 'ELASTIC SEARCH',
    Source.GRAFANA_LOKI: 'GRAFANA LOKI',
    Source.KUBERNETES: 'KUBERNETES',
    Source.SMTP: 'EMAIL SERVER',
    Source.BIG_QUERY: 'BIG QUERY',
    Source.JENKINS: 'JENKINS',
    Source.LINEAR: 'LINEAR',
    Source.GITHUB_ACTIONS: 'GITHUB ACTIONS',
    Source.ARGOCD: 'ARGOCD',
    Source.ROLLBAR: 'ROLLBAR',
}

model_type_display_name_maps = {
    SourceModelType.CLOUDWATCH_METRIC: "Metric",
    SourceModelType.CLOUDWATCH_LOG_GROUP: "Log Group",
    SourceModelType.GRAFANA_TARGET_METRIC_PROMQL: "PromQL",
    SourceModelType.GRAFANA_PROMETHEUS_DATASOURCE: "Data Sources",
    SourceModelType.NEW_RELIC_ENTITY_APPLICATION: "Entity Application",
    SourceModelType.NEW_RELIC_ENTITY_DASHBOARD: "Entity Dashboard",
    SourceModelType.NEW_RELIC_NRQL: "Raw NRQL",
    SourceModelType.CLICKHOUSE_DATABASE: "Database",
    SourceModelType.DATADOG_SERVICE: "Service",
    SourceModelType.DATADOG_QUERY: "Custom Query",
    SourceModelType.EKS_CLUSTER: "Cluster",
    SourceModelType.SQL_DATABASE_CONNECTION_RAW_QUERY: "Query",
    SourceModelType.GRAFANA_MIMIR_PROMQL: "PromQL",
    SourceModelType.POSTGRES_QUERY: "Sql Query",
    SourceModelType.AZURE_WORKSPACE: "Azure Log Analytics Workspace",
    SourceModelType.SSH_SERVER: "SSH Server",
    SourceModelType.OPEN_SEARCH_INDEX: "Open Search Index",
    SourceModelType.GITHUB_REPOSITORY: "Repository",
}

masked_keys_types = [SourceKeyType.DATADOG_APP_KEY,
                     SourceKeyType.DATADOG_API_KEY,
                     SourceKeyType.NEWRELIC_API_KEY,
                     SourceKeyType.NEWRELIC_APP_ID,
                     SourceKeyType.NEWRELIC_QUERY_KEY,
                     SourceKeyType.SENTRY_API_KEY,
                     SourceKeyType.HONEYBADGER_USERNAME,
                     SourceKeyType.HONEYBADGER_PASSWORD,
                     SourceKeyType.HONEYBADGER_PROJECT_ID,
                     SourceKeyType.AWS_ACCESS_KEY,
                     SourceKeyType.AWS_SECRET_KEY,
                     SourceKeyType.AWS_ASSUMED_ROLE_ARN,
                     SourceKeyType.AWS_DRD_CLOUD_ROLE_ARN,
                     SourceKeyType.DATADOG_AUTH_TOKEN,
                     SourceKeyType.AGENT_PROXY_API_KEY,
                     SourceKeyType.GITHUB_TOKEN,
                     SourceKeyType.GOOGLE_CHAT_BOT_OAUTH_TOKEN,
                     SourceKeyType.SLACK_BOT_AUTH_TOKEN,
                     SourceKeyType.AGENT_PROXY_HOST,
                     SourceKeyType.CLICKHOUSE_USER,
                     SourceKeyType.CLICKHOUSE_PASSWORD,
                     SourceKeyType.GCM_PROJECT_ID,
                     SourceKeyType.GCM_PRIVATE_KEY,
                     SourceKeyType.GCM_CLIENT_EMAIL,
                     SourceKeyType.PAGER_DUTY_API_KEY,
                     SourceKeyType.POSTGRES_PASSWORD,
                     SourceKeyType.POSTGRES_USER,
                     SourceKeyType.GRAFANA_API_KEY,
                     SourceKeyType.ZENDUTY_API_KEY,
                     SourceKeyType.OPS_GENIE_API_KEY,
                     SourceKeyType.JIRA_CLOUD_API_KEY,
                     SourceKeyType.ASANA_ACCESS_TOKEN,
                     SourceKeyType.CONFLUENCE_CLOUD_API_KEY,
                     SourceKeyType.NOTION_API_KEY,
                     SourceKeyType.MONGODB_CONNECTION_STRING,
                     SourceKeyType.SQL_DATABASE_CONNECTION_STRING_URI,
                     SourceKeyType.OPEN_SEARCH_PASSWORD,
                     SourceKeyType.JENKINS_API_TOKEN,
                     SourceKeyType.LINEAR_API_KEY,
                     SourceKeyType.GITHUB_ACTIONS_TOKEN,
                     SourceKeyType.SMTP_PASSWORD,
                     SourceKeyType.ARGOCD_TOKEN,
                     SourceKeyType.ROLLBAR_ACCESS_TOKEN,
                     SourceKeyType.CUSTOM_STRATEGIES_ACCOUNT_ID,
                     SourceKeyType.KUBERNETES_CLUSTER_TOKEN,
                     SourceKeyType.KUBERNETES_CLUSTER_CERTIFICATE_AUTHORITY_DATA,
                     SourceKeyType.REMOTE_SERVER_PEM
                     ]
