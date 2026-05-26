.PHONY: push-tag

# Extract version from package.json and push a matching tag to upstream
release:
	@VERSION=$$(node -p "require('./package.json').version") && \
	TAG="v$$VERSION" && \
	git tag "$$TAG" && \
	git push upstream "$$TAG" && \
	echo "Pushed tag $$TAG to upstream"
