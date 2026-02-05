"""
Data exporter: read from primary DB via DataReader; export to CSV/ZIP; single/multiple symbols and export dir management.

Used for: DMS /api/dms/export endpoints; export_dir default "run/exports".

Classes:
    DataExporter  Data exporter

DataExporter methods:
    .export_symbol(symbol, interval, start_date, end_date) -> Optional[str]   Export one symbol to CSV; return path
    .export_all_symbols(symbols, interval, start_date, end_date) -> Optional[str]  Export many to ZIP; return path
    .list_exports() -> List[Dict]   List export dir (name, size, mtime)
    .delete_export(filename) -> bool   Delete export file
    .get_export_path(filename) -> Optional[Path]   Get export file path

DataExporter features:
    - Constructor: reader, export_dir (default "run/exports")
"""

import logging
import os
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class DataExporter:
    """
    Export data from database to CSV files
    """
    
    def __init__(self, reader, export_dir: str = "run/exports"):
        """
        Initialize data exporter
        
        Args:
            reader: DataReader instance
            export_dir: Directory to store export files
        """
        self.reader = reader
        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(exist_ok=True)
        
        logger.info(f"DataExporter initialized: export_dir={self.export_dir}")
    
    def export_symbol(
        self,
        symbol: str,
        interval: str = "1d",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Optional[str]:
        """
        Export single symbol to CSV
        
        Args:
            symbol: Stock symbol
            interval: Time interval
            start_date: Start date (optional)
            end_date: End date (optional)
        
        Returns:
            Path to CSV file, or None if failed
        """
        try:
            # If no dates provided, read all available data
            if start_date is None:
                # Use a very early date as start
                start_date = datetime(2000, 1, 1)
            if end_date is None:
                end_date = datetime.now()
            
            # Read data using read_history method
            data = self.reader.read_history(symbol, start_date, end_date, interval)
            
            if data is None or data.empty:
                logger.warning(f"No data to export for {symbol}")
                return None
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_symbol = symbol.replace(".", "_").replace("/", "_")
            filename = f"{safe_symbol}_{interval}_{timestamp}.csv"
            filepath = self.export_dir / filename
            
            # Export to CSV
            data.to_csv(filepath)
            
            logger.info(f"Exported {symbol}: {len(data)} records to {filename}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Failed to export {symbol}: {e}", exc_info=True)
            return None
    
    def export_all_symbols(
        self,
        symbols: List[str],
        interval: str = "1d",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        create_zip: bool = True,
    ) -> Dict[str, Any]:
        """
        Export all symbols and optionally create ZIP archive
        
        Args:
            symbols: List of symbols to export
            interval: Time interval
            start_date: Start date (optional)
            end_date: End date (optional)
            create_zip: Whether to create ZIP archive
        
        Returns:
            Export result with file paths
        """
        exported_files = []
        failed_symbols = []
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export each symbol (skip special markers)
        for symbol in symbols:
            # Skip special markers
            if symbol in ["*", "all"]:
                continue
            try:
                filepath = self.export_symbol(symbol, interval, start_date, end_date)
                if filepath:
                    exported_files.append(filepath)
                else:
                    failed_symbols.append(symbol)
            except Exception as e:
                logger.error(f"Error exporting {symbol}: {e}")
                failed_symbols.append(symbol)
        
        result = {
            "success": len(failed_symbols) == 0,
            "total_symbols": len(symbols),
            "exported_count": len(exported_files),
            "failed_count": len(failed_symbols),
            "exported_files": exported_files,
            "failed_symbols": failed_symbols,
            "timestamp": timestamp,
        }
        
        # Always create ZIP archive and delete CSV files (only ZIP files are kept)
        if exported_files:
            try:
                zip_filename = f"dms_export_{interval}_{timestamp}.zip"
                zip_filepath = self.export_dir / zip_filename
                
                with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for filepath in exported_files:
                        # Add file to ZIP with just the filename (no path)
                        zipf.write(filepath, Path(filepath).name)
                
                result["zip_file"] = str(zip_filepath)
                result["zip_filename"] = zip_filename
                logger.info(f"Created ZIP archive: {zip_filename} ({len(exported_files)} files)")
                
                # Delete CSV files after creating ZIP archive (only keep ZIP)
                for filepath in exported_files:
                    try:
                        Path(filepath).unlink()
                        logger.debug(f"Deleted CSV file: {filepath}")
                    except Exception as e:
                        logger.error(f"Failed to delete {filepath}: {e}", exc_info=True)
                
            except Exception as e:
                logger.error(f"Failed to create ZIP archive: {e}", exc_info=True)
                result["zip_error"] = str(e)
        
        return result
    
    def list_exports(self) -> List[Dict[str, Any]]:
        """
        List all export files
        
        Returns:
            List of export file info
        """
        files = []
        
        try:
            for filepath in self.export_dir.iterdir():
                if filepath.is_file():
                    stat = filepath.stat()
                    files.append({
                        "filename": filepath.name,
                        "filepath": str(filepath),
                        "size": stat.st_size,
                        "size_mb": round(stat.st_size / (1024 * 1024), 2),
                        "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                        "is_zip": filepath.suffix == ".zip",
                    })
            
            # Sort by creation time (newest first)
            files.sort(key=lambda x: x["created"], reverse=True)
            
        except Exception as e:
            logger.error(f"Failed to list exports: {e}", exc_info=True)
        
        return files
    
    def delete_export(self, filename: str) -> bool:
        """
        Delete an export file
        
        Args:
            filename: File to delete
        
        Returns:
            True if successful
        """
        try:
            filepath = self.export_dir / filename
            
            # Security check: ensure file is in export directory
            if not filepath.resolve().parent == self.export_dir.resolve():
                logger.error(f"Security: attempt to delete file outside export dir: {filename}")
                return False
            
            if filepath.exists() and filepath.is_file():
                filepath.unlink()
                logger.info(f"Deleted export file: {filename}")
                return True
            else:
                logger.warning(f"Export file not found: {filename}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to delete {filename}: {e}", exc_info=True)
            return False
    
    def get_export_path(self, filename: str) -> Optional[Path]:
        """
        Get full path to export file (with security check)
        
        Args:
            filename: File name
        
        Returns:
            Path to file, or None if invalid
        """
        try:
            filepath = self.export_dir / filename
            
            # Security check: ensure file is in export directory
            if not filepath.resolve().parent == self.export_dir.resolve():
                logger.error(f"Security: attempt to access file outside export dir: {filename}")
                return None
            
            if filepath.exists() and filepath.is_file():
                return filepath
            else:
                logger.warning(f"Export file not found: {filename}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get export path for {filename}: {e}", exc_info=True)
            return None
