import time
from sys import stdout
from typing import List, Union
import boto3
import click
from config import TestConfig, ProductionConfig


class ECS:
    def __init__(self, config_obj: Union[TestConfig, ProductionConfig]):
        self.ecs_client = boto3.client('ecs')
        self.as_client = boto3.client('autoscaling')
        self.as_group_name = config_obj.AS_GROUP_NAME
        self.cluster = config_obj.CLUSTER
        self.task_definition = config_obj.TASK_DEFINITION
        self.stable_msg = 'has reached a steady state'

    def get_service_list(self) -> str:
        return self.ecs_client.list_services(
            cluster=self.cluster,
            maxResults=10
        )['serviceArns'][0]

    def get_task_list(self, service_name: str) -> List[str]:
        return self.ecs_client.list_tasks(
            cluster=self.cluster,
            serviceName=service_name
        )['taskArns']

    def describe_clusters(self) -> dict:
        return self.ecs_client.describe_clusters(
            clusters=[
                self.cluster
            ],
        )

    def describe_services(self, service_name: str) -> dict:
        return self.ecs_client.describe_services(
            cluster=self.cluster,
            services=[
                service_name,
            ],
        )

    def check_service_is_stable(self, service_name: str) -> None:
        stdout.write("[*] Check Service status")
        while True:
            current_msg = self.describe_services(service_name=service_name)['services'][0]['events'][0]['message']
            stdout.write(".")
            stdout.flush()
            if self.stable_msg in current_msg:
                print("\n[*] {}".format(current_msg))
                break
            time.sleep(10)

    def check_instance_is_stable(self, desired_capacity: int) -> None:
        stdout.write("[*] Check Instance status")
        while True:
            instance_count = self.describe_clusters()['clusters'][0]['registeredContainerInstancesCount']
            stdout.write(".")
            stdout.flush()
            if instance_count == desired_capacity:
                print("\n[*] Done and Delay 20 seconds")
                time.sleep(20)
                break
            time.sleep(5)

    def check_task_is_stable(self, desired_count: int, service_name: str) -> None:
        stdout.write("[*] Check Task status")
        while True:
            running_count = self.describe_services(service_name=service_name)['services'][0]['runningCount']
            stdout.write(".")
            stdout.flush()
            if desired_count == running_count:
                print("\n[*] Running task count : {}".format(running_count))
                break
            time.sleep(5)

    def describe_task_definition(self) -> str:
        return self.ecs_client.describe_task_definition(
            taskDefinition=self.task_definition,
        )['taskDefinition']['taskDefinitionArn']

    def update_auto_scaling_group(self, min_size: int, desired_capacity: int) -> None:
        print("[*] Update Instance count : {}".format(desired_capacity))
        return self.as_client.update_auto_scaling_group(
            AutoScalingGroupName=self.as_group_name,
            MinSize=min_size,
            MaxSize=desired_capacity,
            DesiredCapacity=desired_capacity,
        )

    def update_service(self, service_name: str, desired_count: int, definition: str) -> None:
        print("[*] Update Service Task count : {}".format(desired_count))
        self.ecs_client.update_service(
            cluster=self.cluster,
            service=service_name,
            desiredCount=desired_count,
            taskDefinition=definition,
            deploymentConfiguration={
                'maximumPercent': 200,
                'minimumHealthyPercent': 100
            },
        )

    def stop_task(self, task_id: str) -> None:
        print("[*] Stop task : {}".format(task_id))
        self.ecs_client.stop_task(
            cluster=self.cluster,
            task=task_id,
            reason='For update'
        )

    def run(self) -> None:
        # 1. 클러스터 서비스 가져오기
        service = self.get_service_list()
        print("[*] Get Cluster Service name : {}".format(service))

        # 2. 서비스 내 실행되고 있는 작업 가져오기
        service_tasks = self.get_task_list(service_name=service)
        all_task_count = len(service_tasks)
        print("[*] Get Running tasks : {}".format(service_tasks))

        # 3. 기존 작업 정의 가져오기
        task_definition = self.describe_task_definition()
        print("[*] Get Task definition : {}".format(task_definition))

        # 4. 서비스 상태 확인
        self.check_service_is_stable(service_name=service)

        # 5. 작업 정의 개수 * 2로 클러스터 인스턴스 개수 증가
        desired_instance_count = int(all_task_count * 2)
        self.update_auto_scaling_group(min_size=all_task_count, desired_capacity=desired_instance_count)

        # 6. 인스턴스 상태 확인
        self.check_instance_is_stable(desired_capacity=desired_instance_count)

        # 7. 작업 정의 개수 * 2로 작업 정의 증가
        self.update_service(service_name=service, desired_count=desired_instance_count, definition=task_definition)
        print("[*] Delay 20 sec After increase task count")
        time.sleep(20)

        # 8. 작업 정의 상태 확인
        self.check_task_is_stable(desired_count=desired_instance_count, service_name=service)

        # 9. 서비스 상태 확인
        self.check_service_is_stable(service_name=service)

        # 10. 기존 작업 정의 중단
        for task in service_tasks:
            self.stop_task(task_id=task)
        print("[*] Delay 10 sec After stop old tasks")
        time.sleep(10)

        # 11. 작업 정의 개수로 인스턴스 개수 조정
        self.update_auto_scaling_group(min_size=all_task_count, desired_capacity=all_task_count)

        # 12. 서비스 원하는 작업 개수 조정
        self.update_service(service_name=service, desired_count=all_task_count, definition=task_definition)

        # 13. 서비스 상태 확인
        self.check_service_is_stable(service_name=service)

        print("[*] Complete Deploy!")


@click.command()
@click.option('--env', default=None, help='prod|test(Deploy environment')
def main(env: str = None):
    if env == 'prod':
        config = ProductionConfig()
    elif env == 'test':
        config = TestConfig()
    else:
        print("Usage: python3 deploy.py --env=prod|test")
        return

    ecs = ECS(config_obj=config)
    ecs.run()


if __name__ == '__main__':
    main()
