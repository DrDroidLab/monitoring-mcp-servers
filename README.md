# Doctor Droid Python Proxy Agent

The present repository contains the source code of the Doctor Droid Python Proxy Agent version 1.0.0.
Read more [here](https://github.com/DrDroidLab/drd-vpc-agent).
![VPC Agent](https://github.com/user-attachments/assets/a17b8904-7811-4597-b4cc-bae34f02cb48)

## Documentation

The Agent runs inside your VPC and acts as a reverse proxy to connect with your metric sources and send
metrics and related data to doctor droid cloud platform. The agent is designed to be lightweight and easy to deploy
with only egress tcp calls to Doctor Droid Cloud Platform.

Currently, the agent supports the following metric sources in your VPC:

* Grafana
* Grafana Loki
* Cloudwatch
* Azure AKS (via native Kubernetes)
* AWS EKS (via AWS/native Kubernetes)
* GKE (via native Kubernetes)
* Kubernetes
* New Relic
* Datadog
* Opensearch
* MongoDB
* Github
* Postgres
* Any SQL Database (via Sql Connection String)

Releasing soon (reach out to us if you need support for these or any other source):

* Bash Commands
* Azure

## Env vars

| Env Var Name        | Description                                    | Required | 
|---------------------|------------------------------------------------|----------|
| DRD_CLOUD_API_TOKEN | Authentication token for doctor droid platform | True     |
| DRD_CLOUD_API_HOST  | API server host for droid platform             | True     |

## Configuration

1. To get started create credentials/secret.yaml file with connections and corresponding credentials.
   Secret format for different connections can be referenced from credentials/credentials_template.yaml.

2. Create an agent token needed for the authenticating http calls between doctor droid platform and agent by
   visiting [site](https://playbooks.drdroid.io/agent-tokens)
   Once auth token is available, you can set the env var as DRD_CLOUD_API_TOKEN=<API_TOKEN>

3. Install via Docker-Compose: To run via docker-compose you will have to clone the github project locally and run this:

```shell
DRD_CLOUD_API_TOKEN=<API_TOKEN> DRD_CLOUD_API_HOST=<API_SERVER_HOST> docker-compose -f agent.docker-compose.yaml up
```

4. Install via Helm Charts
   Pre configuration steps:
   a. Copy and paste the token generated in step 2 in the values.yaml file under var : 'DRD_CLOUD_API_TOKEN'
   Ensure the values are updated in:

   i. helm/charts/celery_beat/values.yaml
   <img width="939" alt="Screenshot 2024-12-20 at 14 03 25" src="https://github.com/user-attachments/assets/0d76245f-ac43-4bfc-a986-54300c826225" />



   ii. helm/charts/celery_worker/values.yaml
   <img width="939" alt="Screenshot 2024-12-20 at 14 03 25" src="https://github.com/user-attachments/assets/0d76245f-ac43-4bfc-a986-54300c826225" />

   b. The secrets for the integrations to be installed are to be added in helm/configmap.yaml file.  
   Refer to the image below for a sample:
   <img width="934" alt="Screenshot 2024-12-20 at 14 02 43" src="https://github.com/user-attachments/assets/cadb2b0a-db0c-4128-bef7-fe2a6288b79b" />

```shell
cd helm
kubectl create namespace drdroid
kubectl apply -f configmap.yaml -n drdroid
helm upgrade --install drd-vpc-agent . -n drdroid
```

In case you are looking to create access for running kubectl commands on this cluster from Doctor Droid platform, run
the following as well.

```shell
kubectl apply -f clusterRole.yaml -n drdroid
kubectl apply -f clusterRoleBinding.yaml -n drdroid
```

## Support

Visit [Doctor Droid website](https://drdroid.io?utm_param=github-py) for getting early access.
Go through our [documentation](https://docs.drdroid.io?utm_param=github-py) to learn more.

For any queries, reach out at [dipesh@drdroid.io](mailto:dipesh@drdroid.io).
