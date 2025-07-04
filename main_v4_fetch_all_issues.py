import requests
import pandas as pd
from datetime import datetime, timedelta
import json
from requests.auth import HTTPBasicAuth
import urllib3
from typing import List, Dict, Optional
import logging

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JiraReleaseExtractor:
    def __init__(self, jira_url: str, username: str, password: str):
        """
        Initialize Jira Release Extractor
        
        Args:
            jira_url (str): Base URL of Jira instance
            username (str): Jira username
            password (str): Jira password/API token
        """
        self.jira_url = jira_url.rstrip('/')
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.verify = False
        
        # Custom field mappings
        self.custom_fields = {
            'sdlc_information': 'customfield_15600',
            'application_name': 'customfield_11700',
            'story_points': 'customfield_10106',
            'sprint': 'customfield_10104',
            'acceptance_criteria': 'customfield_10601',
            'feature_link': 'customfield_10100',
            'notes': 'customfield_10602'
        }
    
    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        """Make API request to Jira"""
        url = f"{self.jira_url}/rest/api/2/{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def get_project_versions(self, project_keys: List[str], start_date: str, end_date: str) -> List[Dict]:
        """
        Fetch released versions for given projects within date range
        
        Args:
            project_keys (List[str]): List of project keys
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            List[Dict]: List of version information
        """
        versions_data = []
        
        for project_key in project_keys:
            logger.info(f"Fetching versions for project: {project_key}")
            
            try:
                # Get all versions for the project
                versions = self._make_request(f"project/{project_key}/versions")
                
                for version in versions:
                    # Check if version is released and within date range
                    if (version.get('released', False) and 
                        version.get('releaseDate')):
                        
                        release_date = datetime.strptime(version['releaseDate'], '%Y-%m-%d')
                        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                        
                        if start_dt <= release_date <= end_dt:
                            version_info = {
                                'project_key': project_key,
                                'version_id': version['id'],
                                'version_name': version['name'],
                                'status': 'Released' if version['released'] else 'Unreleased',
                                'start_date': version.get('startDate', ''),
                                'release_date': version.get('releaseDate', ''),
                                'description': version.get('description', '')
                            }
                            versions_data.append(version_info)
                            
            except Exception as e:
                logger.error(f"Failed to fetch versions for project {project_key}: {e}")
                
        return versions_data
    
    def get_issues_for_version(self, project_key: str, version_name: str) -> List[Dict]:
        """
        Fetch issues for a specific version
        
        Args:
            project_key (str): Project key
            version_name (str): Version name
            
        Returns:
            List[Dict]: List of issue information
        """
        # JQL to find issues fixed in the version
        jql = f'project = "{project_key}" AND fixVersion = "{version_name}"'
        
        # Fields to retrieve
        fields = [
            'key', 'summary', 'issuetype', 'priority', 'status', 'resolution',
            'assignee', 'reporter', 'fixVersions', 'labels', 'description',
            self.custom_fields['sdlc_information'],
            self.custom_fields['application_name'],
            self.custom_fields['story_points'],
            self.custom_fields['sprint'],
            self.custom_fields['acceptance_criteria'],
            self.custom_fields['feature_link'],
            self.custom_fields['notes']
        ]
        
        params = {
            'jql': jql,
            'fields': ','.join(fields),
            'maxResults': 1000,
            'startAt': 0
        }
        
        issues_data = []
        
        try:
            while True:
                response = self._make_request('search', params)
                issues = response.get('issues', [])
                
                if not issues:
                    break
                
                for issue in issues:
                    fields_data = issue.get('fields', {})
                    
                    # Extract sprint information
                    sprint_info = self._extract_sprint_info(fields_data.get(self.custom_fields['sprint']))
                    
                    # Extract fix versions
                    fix_versions = [fv['name'] for fv in fields_data.get('fixVersions', [])]
                    
                    issue_info = {
                        'issue_key': issue['key'],
                        'summary': fields_data.get('summary', ''),
                        'issue_type': fields_data.get('issuetype', {}).get('name', ''),
                        'priority': fields_data.get('priority', {}).get('name', ''),
                        'status': fields_data.get('status', {}).get('name', ''),
                        'resolution': fields_data.get('resolution', {}).get('name', '') if fields_data.get('resolution') else '',
                        'assignee': fields_data.get('assignee', {}).get('displayName', '') if fields_data.get('assignee') else '',
                        'reporter': fields_data.get('reporter', {}).get('displayName', '') if fields_data.get('reporter') else '',
                        'fix_versions': ', '.join(fix_versions),
                        'labels': ', '.join(fields_data.get('labels', [])),
                        'description': fields_data.get('description', ''),
                        'sdlc_information': fields_data.get(self.custom_fields['sdlc_information'], ''),
                        'application_name': fields_data.get(self.custom_fields['application_name'], ''),
                        'story_points': fields_data.get(self.custom_fields['story_points'], ''),
                        'sprint': sprint_info,
                        'acceptance_criteria': fields_data.get(self.custom_fields['acceptance_criteria'], ''),
                        'feature_link': fields_data.get(self.custom_fields['feature_link'], ''),
                        'notes': fields_data.get(self.custom_fields['notes'], ''),
                        'version_name': version_name,
                        'project_key': project_key
                    }
                    issues_data.append(issue_info)
                
                # Check if there are more issues
                if len(issues) < params['maxResults']:
                    break
                
                params['startAt'] += params['maxResults']
                
        except Exception as e:
            logger.error(f"Failed to fetch issues for version {version_name}: {e}")
            
        return issues_data
    
    def _extract_sprint_info(self, sprint_data) -> str:
        """Extract sprint information from sprint field"""
        if not sprint_data:
            return ''
        
        if isinstance(sprint_data, list) and sprint_data:
            # Get the latest sprint
            sprint = sprint_data[-1]
            if isinstance(sprint, str):
                # Parse sprint string format
                if 'name=' in sprint:
                    start = sprint.find('name=') + 5
                    end = sprint.find(',', start)
                    if end == -1:
                        end = sprint.find(']', start)
                    return sprint[start:end] if end > start else ''
            elif isinstance(sprint, dict):
                return sprint.get('name', '')
        
        return str(sprint_data) if sprint_data else ''
    
    def extract_release_data(self, project_keys: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Main method to extract release data
        
        Args:
            project_keys (str): Comma-separated project keys
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: Combined release and issue data
        """
        # Parse project keys
        project_list = [key.strip() for key in project_keys.split(',')]
        
        logger.info(f"Starting extraction for projects: {project_list}")
        logger.info(f"Date range: {start_date} to {end_date}")
        
        # Get versions
        versions_data = self.get_project_versions(project_list, start_date, end_date)
        
        if not versions_data:
            logger.warning("No released versions found in the specified date range")
            return pd.DataFrame()
        
        logger.info(f"Found {len(versions_data)} released versions")
        
        # Get issues for each version
        all_issues = []
        
        for version in versions_data:
            logger.info(f"Fetching issues for version: {version['version_name']}")
            
            issues = self.get_issues_for_version(
                version['project_key'], 
                version['version_name']
            )
            
            # Add version metadata to each issue
            for issue in issues:
                issue.update({
                    'version_id': version['version_id'],
                    'version_status': version['status'],
                    'version_start_date': version['start_date'],
                    'version_release_date': version['release_date'],
                    'version_description': version['description']
                })
            
            all_issues.extend(issues)
            
        if not all_issues:
            logger.warning("No issues found for the specified versions")
            return pd.DataFrame()
        
        # Create DataFrame
        df = pd.DataFrame(all_issues)
        
        # Reorder columns for better readability
        column_order = [
            'project_key', 'version_name', 'version_status', 'version_start_date', 
            'version_release_date', 'version_description', 'issue_key', 'summary', 
            'issue_type', 'priority', 'status', 'resolution', 'assignee', 'reporter',
            'fix_versions', 'labels', 'sdlc_information', 'application_name', 
            'story_points', 'sprint', 'acceptance_criteria', 'feature_link', 
            'notes', 'description'
        ]
        
        # Reorder columns (only include existing columns)
        available_columns = [col for col in column_order if col in df.columns]
        df = df[available_columns]
        
        logger.info(f"Extracted {len(df)} issues from {len(versions_data)} versions")
        
        return df
    
    def export_to_excel(self, df: pd.DataFrame, start_date: str, end_date: str) -> str:
        """
        Export DataFrame to Excel file
        
        Args:
            df (pd.DataFrame): Data to export
            start_date (str): Start date for filename
            end_date (str): End date for filename
            
        Returns:
            str: Filename of exported file
        """
        if df.empty:
            logger.warning("No data to export")
            return None
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        start_str = start_date.replace('-', '')
        end_str = end_date.replace('-', '')
        filename = f"jira_releases_{start_str}_{end_str}_{timestamp}.xlsx"
        
        try:
            # Create Excel writer with multiple sheets
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Main data sheet
                df.to_excel(writer, sheet_name='Release_Data', index=False)
                
                # Summary sheet
                summary_data = []
                for version in df['version_name'].unique():
                    version_df = df[df['version_name'] == version]
                    summary_data.append({
                        'Version': version,
                        'Project': version_df['project_key'].iloc[0],
                        'Release Date': version_df['version_release_date'].iloc[0],
                        'Total Issues': len(version_df),
                        'Issue Types': ', '.join(version_df['issue_type'].value_counts().index.tolist()[:5])
                    })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Adjust column widths
                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width
            
            logger.info(f"Data exported successfully to: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Failed to export data: {e}")
            raise

def main():
    """
    Main function to demonstrate usage
    """
    # Configuration
    JIRA_URL = "https://your-jira-instance.com"  # Replace with your Jira URL
    USERNAME = "your-username"  # Replace with your username
    PASSWORD = "your-password"  # Replace with your password/API token
    
    # Parameters
    PROJECT_KEYS = "PROJ1,PROJ2,PROJ3"  # Replace with your project keys
    START_DATE = "2024-01-01"  # Format: YYYY-MM-DD
    END_DATE = "2024-12-31"    # Format: YYYY-MM-DD
    
    try:
        # Initialize extractor
        extractor = JiraReleaseExtractor(JIRA_URL, USERNAME, PASSWORD)
        
        # Extract data
        df = extractor.extract_release_data(PROJECT_KEYS, START_DATE, END_DATE)
        
        if not df.empty:
            # Export to Excel
            filename = extractor.export_to_excel(df, START_DATE, END_DATE)
            print(f"Release data extracted and saved to: {filename}")
            print(f"Total records: {len(df)}")
        else:
            print("No data found for the specified criteria")
            
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

if __name__ == "__main__":
    main()
