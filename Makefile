all: build_docker build_deployments

build_docker:
	@echo "Building and pushing docker images..."
	cd datalake && docker build -t zboon/pwc-datalake-etl .
	docker push zboon/pwc-datalake-etl
	cd spark && docker build -t zboon/pwc-spark .
	docker push zboon/pwc-spark

build_deployments:
	@echo "Building deployments..."
	python datalake/deployment.py
	python spark/deployment.py
