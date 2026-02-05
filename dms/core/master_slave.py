"""
Master-slave mode: node role (master/slave), URL resolution, status checks, sync request/push.

Used for: DMS slave/master endpoints; role from config; master_config for slave, slaves_config for master.

Classes:
    MasterSlaveManager  Master-slave manager

MasterSlaveManager methods:
    .get_master_url() -> Optional[str]                     Master URL (for slave)
    .get_slave_url(slave_name) -> Optional[str]            Slave URL by name (for master)
    .check_master_status() -> Dict                         Check master status (for slave)
    .check_slave_status(slave_name) -> Dict                Check slave status (for master)
    .request_sync_from_master() -> Dict                    Slave: request sync from master
    .sync_to_slave(slave_name, payload) -> Dict            Master: push data to slave

MasterSlaveManager features:
    - Constructor: role ("master"|"slave"), master_config (for slave), slaves_config (for master)
"""

import logging
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class MasterSlaveManager:
    """
    Master-Slave mode manager
    
    Manages role detection, master-slave communication, and coordination.
    """
    
    def __init__(
        self,
        role: str,
        master_config: Optional[Dict[str, Any]] = None,
        slaves_config: Optional[List[Dict[str, Any]]] = None,
    ):
        """
        Initialize master-slave manager
        
        Args:
            role: Node role ("master" or "slave")
            master_config: Master node configuration (for slave nodes)
            slaves_config: Slaves configuration list (for master nodes)
        """
        self.role = role
        self.master_config = master_config or {}
        self.slaves_config = slaves_config or []
    
    def get_master_url(self) -> Optional[str]:
        """Get master node URL (for slave nodes)"""
        if self.role != "slave":
            return None
        
        if not self.master_config.get("enabled", False):
            return None
        
        host = self.master_config.get("host")
        port = self.master_config.get("port", 11183)
        
        if not host:
            return None
        
        return f"http://{host}:{port}"
    
    def get_slave_url(self, slave_name: str) -> Optional[str]:
        """Get slave node URL by name (for master nodes)"""
        if self.role != "master":
            return None
        
        for slave in self.slaves_config:
            if slave.get("name") == slave_name and slave.get("enabled", True):
                host = slave.get("host")
                port = slave.get("port", 11183)
                return f"http://{host}:{port}"
        
        return None
    
    def check_master_status(self) -> Dict[str, Any]:
        """
        Check master node status (for slave nodes)
        
        Returns:
            Master status dict
        """
        if self.role != "slave":
            return {"status": "not_slave"}
        
        master_url = self.get_master_url()
        if not master_url:
            return {"status": "master_not_configured"}
        
        try:
            response = requests.get(f"{master_url}/api/dms/status", timeout=3)
            if response.status_code == 200:
                return response.json()
            else:
                return {"status": "offline", "error": f"HTTP {response.status_code}"}
        except Exception as e:
            return {"status": "offline", "error": str(e)}
    
    def check_slave_status(self, slave_name: str) -> Dict[str, Any]:
        """
        Check slave node status (for master nodes)
        
        Args:
            slave_name: Slave node name
        
        Returns:
            Slave status dict
        """
        if self.role != "master":
            return {"status": "not_master"}
        
        slave_url = self.get_slave_url(slave_name)
        if not slave_url:
            return {"status": "slave_not_found"}
        
        try:
            response = requests.get(f"{slave_url}/api/dms/status", timeout=3)
            if response.status_code == 200:
                return response.json()
            else:
                return {"status": "offline", "error": f"HTTP {response.status_code}"}
        except Exception as e:
            return {"status": "offline", "error": str(e)}
    
    def request_sync_from_master(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Request sync from master (for slave nodes)
        
        Args:
            symbol: Symbol to sync (None for all)
            start_date: Start date
            end_date: End date
        
        Returns:
            Sync result
        """
        if self.role != "slave":
            return {"success": False, "error": "Not a slave node"}
        
        master_url = self.get_master_url()
        if not master_url:
            return {"success": False, "error": "Master not configured"}
        
        # Get local node identifier
        import socket
        local_host = socket.gethostname()
        local_name = f"{local_host}_slave"
        
        try:
            payload = {}
            if symbol:
                payload["symbol"] = symbol
            if start_date:
                payload["start_date"] = start_date.isoformat()
            if end_date:
                payload["end_date"] = end_date.isoformat()
            
            response = requests.post(
                f"{master_url}/api/dms/slaves/{local_name}/sync",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}",
                }
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def sync_to_slave(
        self,
        slave_name: str,
        symbol: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Sync data to slave (for master nodes)
        
        Args:
            slave_name: Slave node name
            symbol: Symbol to sync
            start_date: Start date
            end_date: End date
        
        Returns:
            Sync result
        """
        if self.role != "master":
            return {"success": False, "error": "Not a master node"}
        
        slave_url = self.get_slave_url(slave_name)
        if not slave_url:
            return {"success": False, "error": f"Slave {slave_name} not found"}
        
        # This would typically be handled by SyncManager
        # This method is for API compatibility
        return {
            "success": True,
            "message": f"Sync request queued for {slave_name}",
        }
