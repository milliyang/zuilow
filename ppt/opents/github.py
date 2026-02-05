"""
GitHub auto-commit: commit timestamp records and proof files to a GitHub repo.

Used for: OpenTimestamps flow; after creating/verifying a timestamp, optionally push record and proof to GitHub.

Functions:
    commit_to_github(record_file, proof_file=None, repo_name=None, branch='main', commit_message=None) -> Dict
        Push record_file (and optional proof_file) to repo; repo_name from GITHUB_REPO env; requires GITHUB_TOKEN.

Features:
    - Requires PyGithub; returns {success, error} or {success, commit_sha, ...}
"""
import os
from pathlib import Path
from typing import Optional, Dict, Any

try:
    from github import Github
    GITHUB_AVAILABLE = True
except ImportError:
    GITHUB_AVAILABLE = False


def commit_to_github(
    record_file: Path,
    proof_file: Optional[Path] = None,
    repo_name: Optional[str] = None,
    branch: str = 'main',
    commit_message: Optional[str] = None
) -> Dict[str, Any]:
    """
    将文件提交到 GitHub 仓库
    
    Args:
        record_file: 原始记录文件路径
        proof_file: 证明文件路径（可选）
        repo_name: GitHub 仓库名 (格式: owner/repo)，从环境变量读取
        branch: 分支名，默认 main
        commit_message: 提交信息，默认自动生成
    
    Returns:
        操作结果字典
    """
    if not GITHUB_AVAILABLE:
        return {
            'success': False,
            'error': 'PyGithub 未安装，请运行: pip install PyGithub'
        }
    
    # 从环境变量读取配置
    github_token = os.getenv('GITHUB_TOKEN')
    if not github_token:
        return {
            'success': False,
            'error': 'GITHUB_TOKEN 环境变量未设置'
        }
    
    if not repo_name:
        repo_name = os.getenv('GITHUB_REPO')
        if not repo_name:
            return {
                'success': False,
                'error': 'GITHUB_REPO 环境变量未设置 (格式: owner/repo)'
            }
    
    if not record_file.exists():
        return {
            'success': False,
            'error': f'记录文件不存在: {record_file}'
        }
    
    try:
        # 初始化 GitHub 客户端
        g = Github(github_token)
        repo = g.get_repo(repo_name)
        
        # 读取文件内容
        with open(record_file, 'rb') as f:
            record_content = f.read()
        
        # 生成提交信息
        if not commit_message:
            date = record_file.stem.replace('record_', '')
            commit_message = f"Add timestamp record for {date}"
        
        # 提交记录文件
        file_path = f"opentimestamps/records/{record_file.name}"
        try:
            # 检查文件是否已存在
            try:
                contents = repo.get_contents(file_path, ref=branch)
                repo.update_file(
                    file_path,
                    commit_message,
                    record_content,
                    contents.sha,
                    branch=branch
                )
                action = 'updated'
            except Exception:
                # 文件不存在，创建新文件
                repo.create_file(
                    file_path,
                    commit_message,
                    record_content,
                    branch=branch
                )
                action = 'created'
        except Exception as e:
            return {
                'success': False,
                'error': f'提交记录文件失败: {str(e)}'
            }
        
        result = {
            'success': True,
            'repo': repo_name,
            'branch': branch,
            'record_file': file_path,
            'action': action,
            'commit_message': commit_message,
        }
        
        # 如果存在证明文件，也一并提交
        if proof_file and proof_file.exists():
            try:
                with open(proof_file, 'rb') as f:
                    proof_content = f.read()
                
                proof_path = f"opentimestamps/proofs/{proof_file.name}"
                try:
                    contents = repo.get_contents(proof_path, ref=branch)
                    repo.update_file(
                        proof_path,
                        f"Update proof file for {record_file.stem}",
                        proof_content,
                        contents.sha,
                        branch=branch
                    )
                    proof_action = 'updated'
                except Exception:
                    repo.create_file(
                        proof_path,
                        f"Add proof file for {record_file.stem}",
                        proof_content,
                        branch=branch
                    )
                    proof_action = 'created'
                
                result['proof_file'] = proof_path
                result['proof_action'] = proof_action
            except Exception as e:
                result['warning'] = f'提交证明文件失败: {str(e)}'
        
        return result
        
    except Exception as e:
        return {
            'success': False,
            'error': f'GitHub 提交失败: {str(e)}'
        }


def auto_commit_after_timestamp(timestamp_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    在创建时间戳后自动提交到 GitHub
    
    Args:
        timestamp_result: create_daily_timestamp() 的返回结果
    
    Returns:
        GitHub 提交结果
    """
    # 检查是否启用 GitHub 自动提交
    if os.getenv('OTS_AUTO_GITHUB', 'false').lower() != 'true':
        return {
            'success': False,
            'skipped': True,
            'message': 'GitHub 自动提交未启用 (设置 OTS_AUTO_GITHUB=true)'
        }
    
    if not timestamp_result.get('success'):
        return {
            'success': False,
            'error': '时间戳创建失败，跳过 GitHub 提交'
        }
    
    record_file = Path(timestamp_result['record_file'])
    proof_file = Path(timestamp_result['proof_file']) if timestamp_result.get('proof_file') else None
    
    return commit_to_github(record_file, proof_file)
