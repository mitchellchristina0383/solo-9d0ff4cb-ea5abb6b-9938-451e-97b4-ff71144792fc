"""FastAPI application exposing the block-storage API.

The interactive Swagger UI at ``/docs`` is the demonstration surface: every
operation needed to import a base, branch writable volumes, read/write/zero
blocks and retire a base can be executed there.  Request models carry examples
so the "Try it out" button works without hand-crafting payloads.

The app is built by :func:`create_app` so the tests can point each instance at
its own SQLite file.  Uvicorn imports the module-level ``app``, configured from
``BLOCKSTORE_DB`` (default ``/data/blockstore.db`` in the container).
"""

from __future__ import annotations

import base64
import binascii
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Body, Depends, FastAPI, Path, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .db import Database, busy_timeout_from_env, database_path_from_env
from .service import BlockStoreService, ServiceError, validate_geometry


def _decode_b64(value: str, what: str = "data_base64") -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        raise ServiceError(422, f"{what} is not valid base64", "invalid_base64") from None


def _encode_b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


# ---------------------------------------------------------------------------
# request models (examples make the Swagger "Try it out" button self-serving)
# ---------------------------------------------------------------------------
class CreateBaseRequest(BaseModel):
    name: str = Field(
        examples=["golden-image"],
        description="Unique base name (letters, digits, '.', '_', '-').",
    )
    block_size: int = Field(
        examples=[8],
        gt=0,
        description="Fixed block size in bytes (1..1048576).",
    )
    blocks_base64: list[str] = Field(
        default_factory=list,
        description=(
            "Optional ordered list of blocks, each base64-encoded and exactly "
            "block_size bytes. Omit to create a base whose blocks are all zero."
        ),
        examples=[["QUFBQUFBQUE=", "QkJCQkJCQkI="]],
    )
    block_count: int | None = Field(
        default=None,
        description=(
            "Number of blocks. Required when blocks_base64 is empty; must equal "
            "len(blocks_base64) otherwise."
        ),
        examples=[2],
    )


class CreateVolumeRequest(BaseModel):
    name: str = Field(examples=["working-copy"])
    base_id: str = Field(
        examples=["4f6b2a1c-0000-4000-8000-000000000000"],
        description="Id of an active base (see GET /bases).",
    )


class WriteBlockRequest(BaseModel):
    data_base64: str = Field(
        description="Block payload, base64-encoded; decoded length must equal block_size.",
        examples=["R0hJSktMTU5P"],
    )


# ---------------------------------------------------------------------------
# dependency
# ---------------------------------------------------------------------------
def get_service(request: Request) -> BlockStoreService:
    return request.app.state.service


Service = Annotated[BlockStoreService, Depends(get_service)]


def create_app(db_path: str | None = None, *, busy_timeout: float | None = None) -> FastAPI:
    database = Database(
        db_path or database_path_from_env(),
        busy_timeout=busy_timeout if busy_timeout is not None else busy_timeout_from_env(),
    )
    block_service = BlockStoreService(database)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database.initialise()
        yield
        database.close_thread_connection()

    app = FastAPI(
        title="Block Store API",
        version="1.0.0",
        description=(
            "Copy-on-write block storage backed by SQLite.\n\n"
            "**Workflow demonstrated in this UI**\n\n"
            "1. `POST /bases` – import a read-only base made of fixed-size blocks.\n"
            "2. `POST /volumes` – create one or more writable volumes from it.\n"
            "3. `GET /volumes/{volume_id}/blocks/{block_index}` – read; unwritten "
            "blocks come from the base.\n"
            "4. `PUT /volumes/{volume_id}/blocks/{block_index}` – write; later "
            "reads return the written bytes.\n"
            "5. `POST /volumes/{volume_id}/blocks/{block_index}/zero` – zero a "
            "block; reads return zeros until it is written again.\n"
            "6. `POST /bases/{base_id}/retirement/prepare` then "
            "`POST /bases/{base_id}/retirement/commit` – remove the base. "
            "Volumes stay writable and every acknowledged write is preserved; "
            "unreferenced blocks are reclaimed for reuse.\n\n"
            "Volumes that inherit the same base block share one stored copy "
            "(content-addressed payloads); references are counted per payload."
        ),
        lifespan=lifespan,
        openapi_tags=[
            {"name": "bases", "description": "Import, inspect and retire read-only bases."},
            {
                "name": "volumes",
                "description": "Create writable volumes and read/write/zero blocks.",
            },
            {
                "name": "operations",
                "description": "Health, statistics, consistency and reclaim audit.",
            },
        ],
    )
    app.state.db = database
    app.state.service = block_service

    @app.exception_handler(ServiceError)
    async def service_error_handler(request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail, "code": exc.code},
        )

    @app.get("/healthz", tags=["operations"], summary="Liveness probe")
    def healthz() -> dict:
        return {"status": "ok"}

    # --------------------------------------------------------------- bases
    @app.post("/bases", status_code=201, tags=["bases"], summary="Import a read-only base")
    def create_base(payload: CreateBaseRequest, svc: Service) -> dict:
        """Import a base made of fixed-size blocks.

        Supply `blocks_base64` to import real content, or set `block_count` and
        omit the list to create an all-zero base.  Duplicate blocks are stored
        once, and volumes created from this base share those stored blocks.
        """
        blocks = [
            _decode_b64(b, f"blocks_base64[{i}]")
            for i, b in enumerate(payload.blocks_base64)
        ]
        if payload.block_count is not None and blocks and payload.block_count != len(blocks):
            raise ServiceError(
                422,
                f"block_count ({payload.block_count}) does not match the number of "
                f"blocks supplied ({len(blocks)})",
                "block_count_mismatch",
            )
        if not blocks:
            count = payload.block_count or 0
            validate_geometry(payload.block_size, count)
            blocks = [bytes(payload.block_size)] * count
        return svc.import_base(payload.name, payload.block_size, blocks)

    @app.get("/bases", tags=["bases"], summary="List bases")
    def list_bases(
        svc: Service,
        status: str | None = Query(
            default=None, pattern="^(active|retiring|retired)$", examples=["active"]
        ),
    ) -> list[dict]:
        return svc.list_bases(status)

    @app.get("/bases/{base_id}", tags=["bases"], summary="Get one base")
    def get_base(svc: Service, base_id: str) -> dict:
        return svc.get_base(base_id)

    @app.get(
        "/bases/{base_id}/blocks/{block_index}",
        tags=["bases"],
        summary="Read a block directly from the base",
    )
    def read_base_block(
        svc: Service, base_id: str, block_index: int = Path(ge=0)
    ) -> dict:
        data = svc.read_base_block(base_id, block_index)
        return {
            "base_id": base_id,
            "block_index": block_index,
            "block_size": len(data),
            "origin": "base",
            "data_base64": _encode_b64(data),
            "is_zero": not any(data),
        }

    @app.get(
        "/bases/{base_id}/retirement",
        tags=["bases"],
        summary="Retirement status and safety report",
    )
    def retirement_status(svc: Service, base_id: str) -> dict:
        return svc.retirement_status(base_id)

    @app.post(
        "/bases/{base_id}/retirement/prepare",
        tags=["bases"],
        summary="Phase 1: mark the base retiring (volumes stay writable)",
    )
    def prepare_retirement(svc: Service, base_id: str) -> dict:
        return svc.prepare_retirement(base_id)

    @app.post(
        "/bases/{base_id}/retirement/cancel",
        tags=["bases"],
        summary="Cancel a prepared retirement",
    )
    def cancel_retirement(svc: Service, base_id: str) -> dict:
        return svc.cancel_retirement(base_id)

    @app.post(
        "/bases/{base_id}/retirement/commit",
        tags=["bases"],
        summary="Phase 2: atomically remove the base and reclaim uninherited blocks",
    )
    def commit_retirement(svc: Service, base_id: str) -> dict:
        return svc.commit_retirement(base_id)

    @app.delete(
        "/bases/{base_id}",
        tags=["bases"],
        summary="Delete the catalogue row of a retired base",
    )
    def delete_base(svc: Service, base_id: str) -> dict:
        return svc.delete_base(base_id)

    # ------------------------------------------------------------- volumes
    @app.post(
        "/volumes", status_code=201, tags=["volumes"], summary="Create a writable volume"
    )
    def create_volume(payload: CreateVolumeRequest, svc: Service) -> dict:
        return svc.create_volume(payload.name, payload.base_id)

    @app.get("/volumes", tags=["volumes"], summary="List volumes")
    def list_volumes(svc: Service) -> list[dict]:
        return svc.list_volumes()

    @app.get("/volumes/{volume_id}", tags=["volumes"], summary="Get one volume")
    def get_volume(svc: Service, volume_id: str) -> dict:
        return svc.get_volume(volume_id)

    @app.get(
        "/volumes/{volume_id}/blocks/{block_index}",
        tags=["volumes"],
        summary="Read a block (base content, zero, or written data)",
    )
    def read_volume_block(
        svc: Service, volume_id: str, block_index: int = Path(ge=0)
    ) -> dict:
        result = svc.read_volume_block(volume_id, block_index)
        result["data_base64"] = _encode_b64(result.pop("data"))
        return result

    @app.get(
        "/volumes/{volume_id}/blocks",
        tags=["volumes"],
        summary="List every block of a volume (bounded to 8 MiB of payload)",
    )
    def list_volume_blocks(
        svc: Service,
        volume_id: str,
        include_data: bool = Query(
            default=True,
            description="Set false to get origin/hash only (useful for large volumes).",
        ),
    ) -> dict:
        return svc.list_volume_blocks(volume_id, include_data=include_data)

    @app.put(
        "/volumes/{volume_id}/blocks/{block_index}",
        tags=["volumes"],
        summary="Write one block (JSON, base64 payload)",
    )
    def write_volume_block(
        svc: Service,
        volume_id: str,
        payload: WriteBlockRequest,
        block_index: int = Path(ge=0),
    ) -> dict:
        return svc.write_volume_block(
            volume_id, block_index, _decode_b64(payload.data_base64)
        )

    @app.put(
        "/volumes/{volume_id}/blocks/{block_index}/raw",
        tags=["volumes"],
        summary="Write one block (raw application/octet-stream body)",
    )
    def write_volume_block_raw(
        svc: Service,
        volume_id: str,
        body: bytes = Body(media_type="application/octet-stream"),
        block_index: int = Path(ge=0),
    ) -> dict:
        return svc.write_volume_block(volume_id, block_index, body)

    @app.post(
        "/volumes/{volume_id}/blocks/{block_index}/zero",
        tags=["volumes"],
        summary="Zero one block (reads zeros until written again)",
    )
    def zero_volume_block(
        svc: Service, volume_id: str, block_index: int = Path(ge=0)
    ) -> dict:
        return svc.zero_volume_block(volume_id, block_index)

    @app.delete("/volumes/{volume_id}", tags=["volumes"], summary="Delete a volume")
    def delete_volume(svc: Service, volume_id: str) -> dict:
        return svc.delete_volume(volume_id)

    # ---------------------------------------------------------- operations
    @app.get("/stats", tags=["operations"], summary="Storage statistics")
    def stats(svc: Service) -> dict:
        return svc.stats()

    @app.get("/verify", tags=["operations"], summary="Run a full consistency audit")
    def verify(svc: Service) -> dict:
        return svc.verify()

    @app.get(
        "/reclaim-events", tags=["operations"], summary="Recent payload reclamations"
    )
    def reclaim_events(svc: Service, limit: int = Query(default=50, ge=1, le=500)) -> list[dict]:
        return svc.reclaim_events(limit)

    return app


app = create_app()
