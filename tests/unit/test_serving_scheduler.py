from scheduler.serving import (
    ServingGPUDeviceSnapshot,
    ServingPlacementReason,
    ServingPlacementRequest,
    ServingWorkerSnapshot,
    choose_single_node_gang_placement,
)


def _worker(
    worker_id: str,
    count: int,
    *,
    model: str = "A100",
    fake: bool = False,
) -> ServingWorkerSnapshot:
    return ServingWorkerSnapshot(
        id=worker_id,
        gpu_devices=tuple(
            ServingGPUDeviceSnapshot(
                uuid=f"{worker_id}-gpu-{index}",
                index=index,
                model=model,
                memory_free_mb=40_960,
                fake=fake,
            )
            for index in range(count)
        ),
    )


def test_tensor_parallel_gang_placement_uses_one_matching_worker() -> None:
    placement, explain = choose_single_node_gang_placement(
        ServingPlacementRequest(
            gpu_count=4,
            gpu_model="FAKE-A100",
            gpu_memory_mb=20_000,
            allow_fake=True,
        ),
        (
            _worker("worker-a", 4, model="FAKE-A100", fake=True),
            _worker("worker-b", 2, model="FAKE-A100", fake=True),
        ),
    )

    assert explain is None
    assert placement is not None
    assert placement.worker_id == "worker-a"
    assert placement.gpu_device_ids == tuple(f"worker-a-gpu-{index}" for index in range(4))


def test_real_serving_placement_excludes_fake_inventory_by_default() -> None:
    placement, explain = choose_single_node_gang_placement(
        ServingPlacementRequest(gpu_count=1, gpu_model="FAKE-A100"),
        (_worker("worker-a", 4, model="FAKE-A100", fake=True),),
    )

    assert placement is None
    assert explain is not None
    assert explain.reason == ServingPlacementReason.INSUFFICIENT_CONTIGUOUS_GPUS


def test_tensor_parallel_never_combines_capacity_across_workers() -> None:
    placement, explain = choose_single_node_gang_placement(
        ServingPlacementRequest(gpu_count=4, gpu_model="A100"),
        (_worker("worker-a", 2), _worker("worker-b", 2)),
    )

    assert placement is None
    assert explain is not None
    assert explain.reason == ServingPlacementReason.INSUFFICIENT_CONTIGUOUS_GPUS
    assert explain.details() == {
        "reason": "INSUFFICIENT_CONTIGUOUS_GPUS",
        "requested_gpu_count": 4,
        "largest_available_worker_gpu_count": 2,
        "requested_gpu_model": "A100",
        "required_gpu_memory_mb": 0,
    }


def test_tensor_parallel_explain_distinguishes_model_and_memory() -> None:
    workers = (_worker("worker-a", 4, model="RTX4090"),)

    placement, model_explain = choose_single_node_gang_placement(
        ServingPlacementRequest(gpu_count=4, gpu_model="A100"), workers
    )
    assert placement is None
    assert model_explain is not None
    assert model_explain.reason == ServingPlacementReason.GPU_MODEL_MISMATCH

    placement, memory_explain = choose_single_node_gang_placement(
        ServingPlacementRequest(gpu_count=4, gpu_model="RTX4090", gpu_memory_mb=80_000),
        workers,
    )
    assert placement is None
    assert memory_explain is not None
    assert memory_explain.reason == ServingPlacementReason.INSUFFICIENT_GPU_MEMORY
