"""
Pruebas Unitarias para el Módulo de Sanitización (Integrante 1).
"""

import argparse

import pytest
from src.triton_telemetry.sanitizer import (
    validate_cluster_id,
    validate_timeout,
)


class TestTimeoutValidator:
    """Pruebas para el validador de límites de tiempo."""

    @pytest.mark.parametrize(
        "valid_val, expected",
        [
            ("0.1", 0.1),
            ("1.0", 1.0),
            ("2.5", 2.5),
            ("3.0", 3.0),
            ("5.0", 5.0),
            (" 4.2 ", 4.2),
        ],
    )
    def test_validate_timeout_valid_range(self, valid_val, expected):
        assert validate_timeout(valid_val) == pytest.approx(expected)

    @pytest.mark.parametrize(
        "invalid_val",
        [
            "0.09",
            "0.0",
            "-1.0",
            "5.01",
            "10.0",
            "999",
        ],
    )
    def test_validate_timeout_out_of_bounds(self, invalid_val):
        with pytest.raises(argparse.ArgumentTypeError) as exc_info:
            validate_timeout(invalid_val)
        assert "estrictamente acotado" in str(exc_info.value)

    @pytest.mark.parametrize(
        "non_numeric",
        [
            "abc",
            "diez",
            "",
            "None",
            "1.2.3",
        ],
    )
    def test_validate_timeout_non_numeric(self, non_numeric):
        with pytest.raises(argparse.ArgumentTypeError) as exc_info:
            validate_timeout(non_numeric)
        assert "número decimal válido" in str(exc_info.value)


class TestClusterIdValidator:
    """Pruebas para el validador de identificadores de clúster."""

    @pytest.mark.parametrize(
        "valid_cluster",
        [
            "cluster-us-east-01",
            "cluster-us-west-02",
            "cluster-eu-central-1",
            "cluster-sa-east-99",
            "cluster-ap-southeast-10",
            "cluster-prod-aws-us-east-1",
        ],
    )
    def test_validate_cluster_id_valid(self, valid_cluster):
        assert validate_cluster_id(valid_cluster) == valid_cluster

    @pytest.mark.parametrize(
        "invalid_cluster",
        [
            "cluster-invalido-id",
            "cluster_us_east_01",
            "cluster-us-east",
            "cluster-01",
            "prod-cluster-us-east-01",
            "cluster-us-east-01-extra",
            "",
            "   ",
            "123-cluster",
        ],
    )
    def test_validate_cluster_id_invalid(self, invalid_cluster):
        with pytest.raises(argparse.ArgumentTypeError) as exc_info:
            validate_cluster_id(invalid_cluster)
        assert "Identificador de clúster inválido" in str(exc_info.value)
