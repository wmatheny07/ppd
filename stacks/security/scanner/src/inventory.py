from __future__ import annotations

import logging

import docker

logger = logging.getLogger(__name__)


def get_running_images() -> list[dict]:
    """
    Query the Docker socket for all running containers and deduplicate by image ref.
    Returns one entry per unique image currently in use.
    """
    client = docker.from_env()
    seen: set[str] = set()
    images: list[dict] = []

    for container in client.containers.list():
        image_ref: str = container.attrs["Config"]["Image"]

        if image_ref in seen:
            continue
        seen.add(image_ref)

        if ":" in image_ref and not image_ref.startswith("sha256:"):
            name, tag = image_ref.rsplit(":", 1)
        else:
            name, tag = image_ref, "latest"

        images.append(
            {
                "container_name": container.name,
                "image_name": name,
                "image_tag": tag,
                "image_ref": image_ref,
                "image_id": container.attrs["Image"][:12],
            }
        )
        logger.debug("Discovered image %s (container: %s)", image_ref, container.name)

    logger.info("Discovered %d unique images across running containers", len(images))
    return images
