all: build_docker build_deployments

build_docker:
	@echo "Building and pushing docker images..."
	cd datalake && docker build -t $(datalake_docker_image_name) .
	docker push $(datalake_docker_image_name)
	cd spark && docker build -t $(spark_docker_image_name) .
	docker push $(spark_docker_image_name)

build_deployments:
	@echo "Building deployments..."
	python datalake/deployment.py
	python spark/deployment.py
