docker_image := iomete/iomete_kafka_streaming
docker_tag := 0.1.8

test:
	python setup.py test

install-requirements:
	python setup.py install

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f docker/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}
