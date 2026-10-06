from decimal import Decimal

from aiohttp.test_utils import TestClient, TestServer
import pytest

from ckpool_share_exporter.server.app import create_app
from ckpool_share_exporter.settings import settings


class DistributionDAO:
    def __init__(self):
        self.call = None

    async def distribution(self, username, pool_instance, window_days=None):
        self.call = (username, pool_instance, window_days)
        return [{
            "workername": f"{username}.rig1",
            "diff_sum": 42.0,
            "part": 1.0,
            "shares_ok": 2,
            "shares_ko": 1,
        }]


@pytest.fixture
async def http_client():
    dao = DistributionDAO()
    client = TestClient(TestServer(create_app(dao)))
    await client.start_server()
    try:
        yield client, dao
    finally:
        await client.close()


async def test_distribution_returns_dao_rows_and_uses_default_window(http_client):
    client, dao = http_client

    response = await client.get("/v1/distribution/bc1qminer")

    assert response.status == 200
    assert await response.json() == [{
        "workername": "bc1qminer.rig1",
        "diff_sum": 42.0,
        "part": 1.0,
        "shares_ok": 2,
        "shares_ko": 1,
    }]
    assert dao.call == ("bc1qminer", settings.pool_instance_name, settings.distribution_window_days)


async def test_health_is_available_outside_the_versioned_api(http_client):
    client, _dao = http_client

    response = await client.get("/health")

    assert response.status == 200
    assert await response.json() == {"status": "ok"}


async def test_distribution_accepts_a_window_days_query_parameter(http_client):
    client, dao = http_client

    response = await client.get("/v1/distribution/bc1qminer?window_days=7")

    assert response.status == 200
    assert dao.call == ("bc1qminer", settings.pool_instance_name, 7)


async def test_distribution_serializes_decimal_values_as_json_numbers(http_client):
    client, dao = http_client
    dao.distribution = async_decimal_distribution

    response = await client.get("/v1/distribution/bc1qminer")

    assert response.status == 200
    assert await response.json() == [{
        "workername": "bc1qminer.rig1",
        "diff_sum": 12.5,
        "part": 0.25,
        "shares_ok": 3,
        "shares_ko": 1,
    }]


async def async_decimal_distribution(username, pool_instance, window_days=None):
    return [{
        "workername": f"{username}.rig1",
        "diff_sum": Decimal("12.5"),
        "part": Decimal("0.25"),
        "shares_ok": 3,
        "shares_ko": 1,
    }]


@pytest.mark.parametrize("window_days", ["0", "-1", "not-a-number"])
async def test_distribution_rejects_invalid_window_days(http_client, window_days):
    client, dao = http_client

    response = await client.get(f"/v1/distribution/bc1qminer?window_days={window_days}")

    assert response.status == 400
    assert dao.call is None
