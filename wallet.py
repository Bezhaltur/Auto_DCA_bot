"""
Wallet keystore management.
User provides standard Ethereum JSON keystore files.
Uses eth_account.Account.decrypt for decryption.
"""

import json
import os
from typing import Optional
from eth_account import Account
import logging

logger = logging.getLogger(__name__)

# Directory for keystore files
KEYSTORE_DIR = "keystores"
os.makedirs(KEYSTORE_DIR, exist_ok=True)


def generate_keystore_path(user_id: int, network_key: str) -> str:
    """
    Generate keystore file path for user and network.
    
    Args:
        user_id: Telegram user ID
        network_key: Network key (e.g., "USDT-ARB")
    
    Returns:
        Path to keystore file
    """
    safe_network = network_key.replace("-", "_").lower()
    filename = f"user_{user_id}_{safe_network}.json"
    return os.path.join(KEYSTORE_DIR, filename)


def save_keystore(keystore: dict, user_id: int, network_key: str) -> str:
    """
    Save keystore to file.
    
    Args:
        keystore: Keystore dictionary (standard Ethereum JSON format)
        user_id: Telegram user ID
        network_key: Network key
    
    Returns:
        Path to saved keystore file
    """
    filepath = generate_keystore_path(user_id, network_key)
    
    with open(filepath, "w") as f:
        json.dump(keystore, f, indent=2)
    
    # Set restrictive permissions (owner read/write only)
    os.chmod(filepath, 0o600)
    
    logger.info(f"Keystore saved to {filepath}")
    return filepath


def load_keystore(user_id: int, network_key: str) -> Optional[dict]:
    """
    Load keystore from file.
    
    Args:
        user_id: Telegram user ID
        network_key: Network key
    
    Returns:
        Keystore dictionary or None if not found
    """
    filepath = generate_keystore_path(user_id, network_key)
    
    if not os.path.exists(filepath):
        return None
    
    try:
        with open(filepath, "r") as f:
            keystore = json.load(f)
        return keystore
    except Exception as e:
        logger.error(f"Error loading keystore from {filepath}: {e}")
        return None


def load_keystore_from_file(filepath: str) -> dict:
    """
    Load keystore from external file path.
    
    Args:
        filepath: Path to keystore JSON file
    
    Returns:
        Keystore dictionary
    
    Raises:
        ValueError: If file doesn't exist or is invalid
    """
    if not os.path.exists(filepath):
        raise ValueError(f"Keystore file not found: {filepath}")
    
    try:
        with open(filepath, "r") as f:
            keystore = json.load(f)
        return keystore
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in keystore file: {e}")
    except Exception as e:
        raise ValueError(f"Error reading keystore file: {e}")


def load_keystore_from_json(keystore_json: str) -> dict:
    """
    Load keystore from JSON string.
    
    Args:
        keystore_json: JSON string of keystore
    
    Returns:
        Keystore dictionary
    
    Raises:
        ValueError: If JSON is invalid
    """
    try:
        keystore = json.loads(keystore_json)
        return keystore
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON keystore: {e}")


def decrypt_private_key(keystore: dict, password: str) -> str:
    """
    Decrypt private key from keystore using eth_account.Account.decrypt.
    
    Args:
        keystore: Keystore dictionary (standard Ethereum JSON format)
        password: Decryption password
    
    Returns:
        Private key (hex string with 0x prefix)
    
    Raises:
        ValueError: If password is incorrect or keystore is invalid
    """
    try:
        # Use eth_account.Account.decrypt (standard method)
        private_key = Account.decrypt(keystore, password)
        return private_key.hex()
    except Exception as e:
        logger.error(f"Error decrypting private key: {e}")
        raise ValueError(f"Incorrect password or invalid keystore: {e}")


def get_wallet_address(keystore: dict, password: str) -> str:
    """
    Get wallet address from keystore.
    
    Args:
        keystore: Keystore dictionary
        password: Decryption password
    
    Returns:
        Wallet address (checksummed)
    """
    private_key = decrypt_private_key(keystore, password)
    account = Account.from_key("0x" + private_key)
    return account.address


def delete_keystore(user_id: int, network_key: str) -> bool:
    """
    Delete keystore file.
    
    Args:
        user_id: Telegram user ID
        network_key: Network key
    
    Returns:
        True if deleted, False if not found
    """
    filepath = generate_keystore_path(user_id, network_key)
    
    if os.path.exists(filepath):
        os.remove(filepath)
        logger.info(f"Keystore deleted: {filepath}")
        return True
    
    return False


def keystore_exists(user_id: int, network_key: str) -> bool:
    """Check if keystore exists for user and network."""
    filepath = generate_keystore_path(user_id, network_key)
    return os.path.exists(filepath)
