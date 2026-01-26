"""
Test configuration and mode management.
Handles dry-run, mock FixedFloat, and testnet modes.
"""

import os
import logging
from typing import Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# Test modes from environment
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
MOCK_FIXEDFLOAT = os.getenv("MOCK_FIXEDFLOAT", "false").lower() == "true"
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() == "true"

# Log test mode status
if DRY_RUN:
    logger.warning("⚠️ DRY_RUN MODE ENABLED - No transactions will be broadcast")
if MOCK_FIXEDFLOAT:
    logger.warning("⚠️ MOCK_FIXEDFLOAT MODE ENABLED - Using mocked API responses")
if USE_TESTNET:
    logger.warning("⚠️ USE_TESTNET MODE ENABLED - Using testnet networks")


def is_test_mode() -> bool:
    """Check if any test mode is enabled."""
    return DRY_RUN or MOCK_FIXEDFLOAT or USE_TESTNET


def get_mock_fixedfloat_order(network_key: str, amount: float, btc_address: str) -> Dict[str, Any]:
    """
    Generate mock FixedFloat order response.
    
    Args:
        network_key: Network key (e.g., "USDT-ARB")
        amount: Order amount
        btc_address: BTC destination address
    
    Returns:
        Mock order data matching FixedFloat API format
    """
    import secrets
    
    # Generate mock order ID
    mock_order_id = f"TEST{secrets.token_hex(8).upper()}"
    
    # Generate mock deposit address (valid EVM address format)
    mock_deposit = "0x" + secrets.token_hex(20)
    
    # Generate mock BTC transaction ID
    mock_btc_txid = secrets.token_hex(32)
    
    # Mock response structure matching FixedFloat API
    return {
        "code": 0,
        "msg": "success",
        "data": {
            "id": mock_order_id,
            "type": "fixed",
            "from": {
                "code": network_key.replace("USDT-", ""),
                "network": network_key,
                "amount": str(amount),
                "address": mock_deposit,
            },
            "to": {
                "code": "BTC",
                "network": "BITCOIN",
                "amount": "0.001",  # Mock BTC amount
                "address": btc_address,
            },
            "time": {
                "left": 3600,  # 1 hour
                "expired": False,
            },
            "status": "WAIT",
        }
    }


def get_mock_fixedfloat_ccies() -> Dict[str, Any]:
    """Generate mock FixedFloat ccies response."""
    return {
        "code": 0,
        "msg": "success",
        "data": [
            {
                "coin": "USDT",
                "code": "USDTARBITRUM",
                "network": "Arbitrum",
                "status": "active",
            },
            {
                "coin": "USDT",
                "code": "USDTBSC",
                "network": "BSC",
                "status": "active",
            },
            {
                "coin": "USDT",
                "code": "USDTMATIC",
                "network": "Polygon",
                "status": "active",
            },
        ]
    }


def get_mock_fixedfloat_price(network_key: str) -> Dict[str, Any]:
    """Generate mock FixedFloat price response."""
    return {
        "code": 0,
        "msg": "success",
        "data": {
            "from": {
                "code": network_key.replace("USDT-", ""),
                "min": "10.0",
                "max": "500.0",
            },
            "to": {
                "code": "BTC",
                "amount": "0.0001",  # Mock BTC amount
            },
        }
    }


def mask_sensitive_data(data: Any) -> Any:
    """
    Mask sensitive data in logs.
    Replaces private keys, passwords, and addresses with masked versions.
    
    Args:
        data: Data to mask (dict, list, str, etc.)
    
    Returns:
        Masked data
    """
    if isinstance(data, dict):
        masked = {}
        for key, value in data.items():
            key_lower = str(key).lower()
            # Mask sensitive keys
            if any(sensitive in key_lower for sensitive in ['password', 'private', 'secret', 'key']):
                masked[key] = "***MASKED***"
            elif key_lower == 'address' and isinstance(value, str) and len(value) > 10:
                # Mask addresses but keep first/last chars
                masked[key] = f"{value[:6]}...{value[-4:]}"
            else:
                masked[key] = mask_sensitive_data(value)
        return masked
    elif isinstance(data, list):
        return [mask_sensitive_data(item) for item in data]
    elif isinstance(data, str):
        # Mask if looks like private key
        if len(data) == 66 and data.startswith("0x"):
            return "0x" + "***" * 20
        return data
    else:
        return data
