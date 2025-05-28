import requests
import pandas as pd
from datetime import datetime
import json
from typing import List, Dict, Optional, Tuple
import logging
from requests.auth import HTTPBasicAuth
import urllib3

# Disable SSL warnings when verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JiraReleaseFetcher:
    def __init__(self, jira_url: str, username: str, password: str):
        """
        Initialize Jira API connection with basic authentication
        
        Args:
            jira_url: Base URL of your Jira instance (e.g., 'https://your-domain.atlassian.net')
            username: Your Jira username
            password: Your Jira password
        """
        self.jira_url = jira_url.rstrip('/')
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.verify = False  # Disable SSL verification
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make API request to Jira"""
        url = f"{self.jira_url}/rest/api/3/{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def get_projects(self) -> List[Dict]:
        """Get all projects to identify project keys"""
        return self._make_request('project')
    
    def get_releases_in_date_range(self, project_key: str, start_date: str, end_date: str) -> List[Dict]:
        """
        Fetch releases with status 'released' within the specified date range
        
        Args:
            project_key: Jira project key
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        
        Returns:
            List of release dictionaries
        """
        try:
            # Get all versions for the project
            versions_data = self._make_request(f'project/{project_key}/version')
            
            released_versions = []
            for version in versions_data:
                # Check if version is released and has a release date
                if (version.get('released', False) and 
                    version.get('releaseDate')):
                    
                    release_date = datetime.strptime(version['releaseDate'], '%Y-%m-%d')
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                    
                    # Check if release date is within range
                    if start_dt <= release_date <= end_dt:
                        released_versions.append(version)
            
            logger.info(f"Found {len(released_versions)} releases in date range")
            return released_versions
            
        except Exception as e:
            logger.error(f"Error fetching releases: {e}")
            return []
    
    def get_issues_for_version(self, project_key: str, version_name: str) -> List[Dict]:
        """
        Fetch all issues associated with a specific version
        
        Args:
            project_key: Jira project key
            version_name: Name of the version/release
        
        Returns:
            List of issue dictionaries
        """
        try:
            # JQL query to find issues with the specific fix version
            jql = f'project = "{project_key}" AND fixVersion = "{version_name}"'
            
            # Get all issues with detailed fields
            params = {
                'jql': jql,
                'fields': 'key,summary,description,assignee,reporter,status,resolution,priority,issuetype,fixVersions,labels,customfield_*,sprint,components',
                'maxResults': 1000,
                'startAt': 0
            }
            
            all_issues = []
            while True:
                response = self._make_request('search', params)
                issues = response.get('issues', [])
                all_issues.extend(issues)
                
                # Check if there are more results
                if len(issues) < params['maxResults']:
                    break
                params['startAt'] += params['maxResults']
            
            logger.info(f"Found {len(all_issues)} issues for version {version_name}")
            return all_issues
            
        except Exception as e:
            logger.error(f"Error fetching issues for version {version_name}: {e}")
            return []
    
    def extract_issue_data(self, issue: Dict) -> Dict:
        """
        Extract relevant fields from issue data
        
        Args:
            issue: Raw issue data from Jira API
        
        Returns:
            Dictionary with extracted and formatted issue data
        """
        fields = issue.get('fields', {})
        
        # Extract basic fields
        issue_data = {
            'issue_key': issue.get('key', ''),
            'summary': fields.get('summary', ''),
            'description': fields.get('description', {}).get('content', [{}])[0].get('content', [{}])[0].get('text', '') if fields.get('description') else '',
            'priority': fields.get('priority', {}).get('name', '') if fields.get('priority') else '',
            'issue_type': fields.get('issuetype', {}).get('name', '') if fields.get('issuetype') else '',
            'assignee': fields.get('assignee', {}).get('displayName', '') if fields.get('assignee') else '',
            'reporter': fields.get('reporter', {}).get('displayName', '') if fields.get('reporter') else '',
            'status': fields.get('status', {}).get('name', '') if fields.get('status') else '',
            'resolution': fields.get('resolution', {}).get('name', '') if fields.get('resolution') else '',
            'labels': ', '.join(fields.get('labels', [])),
            'fix_versions': ', '.join([v.get('name', '') for v in fields.get('fixVersions', [])]),
        }
        
        # Extract custom fields (these field IDs may vary by Jira instance)
        # You'll need to map these to your actual custom field IDs
        custom_fields_mapping = {
            'story_points': 'customfield_10016',  # Common field ID for story points
            'sprint': 'customfield_10020',        # Common field ID for sprint
            'acceptance_criteria': 'customfield_10030',  # Example custom field
            'feature_link': 'customfield_10040',  # Example custom field
            'notes': 'customfield_10050',         # Example custom field
            'sdlc_information': 'customfield_10060',  # Example custom field
            'application_name': 'customfield_10070',  # Example custom field
        }
        
        for field_name, field_id in custom_fields_mapping.items():
            field_value = fields.get(field_id, '')
            if isinstance(field_value, dict):
                field_value = field_value.get('value', '') or field_value.get('name', '')
            elif isinstance(field_value, list):
                field_value = ', '.join([str(item.get('name', item)) if isinstance(item, dict) else str(item) for item in field_value])
            issue_data[field_name] = str(field_value) if field_value else ''
        
        return issue_data
    
    def fetch_releases_and_issues_multiple_projects(self, project_keys_str: str, start_date: str, end_date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Main method to fetch releases and associated issues for multiple projects
        
        Args:
            project_keys_str: Comma-separated string of project keys (e.g., "PROJ1,PROJ2,PROJ3")
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        
        Returns:
            Tuple of (releases_dataframe, issues_dataframe)
        """
        # Parse project keys
        project_keys = [key.strip() for key in project_keys_str.split(',') if key.strip()]
        logger.info(f"Processing projects: {project_keys}")
        
        all_releases_data = []
        all_issues_data = []
        
        for project_key in project_keys:
            logger.info(f"Fetching releases for project {project_key} between {start_date} and {end_date}")
            
            try:
                # Get releases in date range
                releases = self.get_releases_in_date_range(project_key, start_date, end_date)
                
                for release in releases:
                    # Prepare release data
                    release_data = {
                        'version': release.get('name', ''),
                        'status': 'Released' if release.get('released', False) else 'Unreleased',
                        'start_date': release.get('startDate', ''),
                        'release_date': release.get('releaseDate', ''),
                        'description': release.get('description', ''),
                        'project_key': project_key
                    }
                    all_releases_data.append(release_data)
                    
                    # Get issues for this release
                    issues = self.get_issues_for_version(project_key, release.get('name', ''))
                    
                    for issue in issues:
                        issue_data = self.extract_issue_data(issue)
                        issue_data['release_version'] = release.get('name', '')
                        issue_data['release_date'] = release.get('releaseDate', '')
                        issue_data['project_key'] = project_key
                        all_issues_data.append(issue_data)
                        
            except Exception as e:
                logger.error(f"Error processing project {project_key}: {e}")
                continue
        
        # Create DataFrames
        releases_df = pd.DataFrame(all_releases_data)
        issues_df = pd.DataFrame(all_issues_data)
        
        # Reorder columns for better readability
        if not releases_df.empty:
            releases_df = releases_df[['project_key', 'version', 'status', 'start_date', 'release_date', 'description']]
        
        if not issues_df.empty:
            issues_columns = [
                'project_key', 'release_version', 'release_date', 'issue_key', 'summary', 
                'priority', 'issue_type', 'assignee', 'reporter', 'status', 'resolution',
                'fix_versions', 'labels', 'sdlc_information', 'application_name', 
                'story_points', 'sprint', 'acceptance_criteria', 'feature_link', 
                'notes', 'description'
            ]
            # Only include columns that exist in the dataframe
            existing_columns = [col for col in issues_columns if col in issues_df.columns]
            issues_df = issues_df[existing_columns]
        
        logger.info(f"Created dataframes: {len(releases_df)} releases, {len(issues_df)} issues across all projects")
        return releases_df, issues_df
    
    def export_to_excel(self, releases_df: pd.DataFrame, issues_df: pd.DataFrame, 
                       project_keys_str: str, start_date: str, end_date: str) -> str:
        """
        Export dataframes to Excel file with proper naming convention
        
        Args:
            releases_df: DataFrame containing release data
            issues_df: DataFrame containing issues data
            project_keys_str: Comma-separated project keys for filename
            start_date: Start date for filename
            end_date: End date for filename
        
        Returns:
            Filename of the created Excel file
        """
        try:
            # Create filename with proper naming convention
            project_names = "_".join([key.strip() for key in project_keys_str.split(',') if key.strip()])
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            start_formatted = start_date.replace('-', '')
            end_formatted = end_date.replace('-', '')
            
            filename = f"Jira_Releases_Issues_{project_names}_{start_formatted}_to_{end_formatted}_{timestamp}.xlsx"
            
            # Create Excel writer object
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Write releases data to first sheet
                if not releases_df.empty:
                    releases_df.to_excel(writer, sheet_name='Releases', index=False)
                    
                    # Auto-adjust column widths for releases sheet
                    worksheet = writer.sheets['Releases']
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)  # Max width of 50
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                
                # Write issues data to second sheet
                if not issues_df.empty:
                    issues_df.to_excel(writer, sheet_name='Issues', index=False)
                    
                    # Auto-adjust column widths for issues sheet
                    worksheet = writer.sheets['Issues']
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)  # Max width of 50
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                
                # Create summary sheet
                summary_data = {
                    'Metric': [
                        'Total Projects Processed',
                        'Total Releases Found',
                        'Total Issues Found',
                        'Date Range Start',
                        'Date Range End',
                        'Export Timestamp'
                    ],
                    'Value': [
                        len([key.strip() for key in project_keys_str.split(',') if key.strip()]),
                        len(releases_df),
                        len(issues_df),
                        start_date,
                        end_date,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Auto-adjust column widths for summary sheet
                worksheet = writer.sheets['Summary']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 30)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            logger.info(f"Data exported to Excel file: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error exporting to Excel: {e}")
            raise

def main():
    """
    Example usage of the JiraReleaseFetcher
    """
    # Configuration
    JIRA_URL = "https://your-domain.atlassian.net"  # Replace with your Jira URL
    USERNAME = "your-email@example.com"              # Replace with your email
    API_TOKEN = "your-api-token"                     # Replace with your API token
    PROJECT_KEY = "YOUR_PROJECT"                     # Replace with your project key
    
    # Date range
    START_DATE = "2024-01-01"
    END_DATE = "2024-12-31"
    
    try:
        # Initialize fetcher
        fetcher = JiraReleaseFetcher(JIRA_URL, USERNAME, API_TOKEN)
        
        # Fetch data
        releases_df, issues_df = fetcher.fetch_releases_and_issues(
            project_key=PROJECT_KEY,
            start_date=START_DATE,
            end_date=END_DATE
        )
        
        # Display results
        print("Releases DataFrame:")
        print(releases_df.head())
        print(f"\nTotal releases: {len(releases_df)}")
        
        print("\nIssues DataFrame:")
        print(issues_df.head())
        print(f"\nTotal issues: {len(issues_df)}")
        
        # Save to CSV files
        releases_df.to_csv('jira_releases.csv', index=False)
        issues_df.to_csv('jira_issues.csv', index=False)
        
        print("\nDataFrames saved to CSV files:")
        print("- jira_releases.csv")
        print("- jira_issues.csv")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()
