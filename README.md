# Doctor Droid Python Proxy Agent

The present repository contains the source code of the Doctor Droid Python Proxy Agent version 1.0.0.
Read more [here](https://github.com/DrDroidLab/drd-vpc-agent).

## Documentation

The Agent runs inside your VPC and acts as a reverse proxy to connect with your metric sources and send
metrics and related data to doctor droid cloud platform.

Currently, the agent supports the following metric sources in your VPC:

* Grafana
* Cloudwatch
* Azure
* EKS
* GKE
* Kubernetes
* Bash Commands
* New Relic
* Datadog

## Env vars

| Env Var Name        | Description                                    | Required | 
|---------------------|------------------------------------------------|----------|
| DRD_CLOUD_API_TOKEN | Authentication token for doctor droid platform | True     |
| DRD_CLOUD_API_HOST  | API server host for droid platform             | True     |

## Configuration

1. To get started create credentials/secret.yaml file with connections and corresponding credentials.
Secret format for different connections can be referenced from credentials/credentials_template.yaml. 

2. Identify the auth token needed for the authenticating http calls between doctor droid platform and agent by
visiting [site](https://playbooks.drdroid.io/api-keys)
Once auth token is available, you can set the env var as:

```shell
DRD_CLOUD_API_TOKEN=<API_TOKEN> DRD_CLOUD_API_HOST=<API_SERVER_HOST> docker-compose -f agent.docker-compose.yml up
```

## Support

Visit [Doctor Droid website](https://drdroid.io?utm_param=github-py) for getting early access.
Go through our [documentation](https://docs.drdroid.io?utm_param=github-py) to learn more.

For any queries, reach out at [dipesh@drdroid.io](mailto:dipesh@drdroid.io).
