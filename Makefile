docker_image := iomete/iomete_kafka_streaming
docker_tag := 0.1.1

test:
	python setup.py test

install-requirements:
	python setup.py install

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f docker/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}

submit:
	/Users/namig/IdeaProjects/infra/spark/spark-google-operator/docker/public-spark-3.2.1/spark/spark3.2.1-iomete-release-v4/bin/spark-submit \
	    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
		driver.py