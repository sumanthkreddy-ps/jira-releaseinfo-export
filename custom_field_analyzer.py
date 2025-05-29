import requests
import json
import pandas as pd
from urllib3.exceptions import InsecureRequestWarning
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(InsecureRequestWarning)

class JiraFieldAnalyzer:
    def __init__(self, jira_url, username, password):
        """
        Initialize Jira API client for field analysis
        
        Args:
            jira_url (str): Base URL of Jira instance
            username (str): Jira username
            password (str): Jira password/API token
        """
        self.jira_url = jira_url.rstrip('/')
        self.auth = (username, password)
        self.session = requests.Session()
        self.session.verify = False
    
    def get_all_field_mappings(self):
        """Get all field mappings from Jira"""
        url = f"{self.jira_url}/rest/api/2/field"
        
        try:
            response = self.session.get(url, auth=self.auth)
            response.raise_for_status()
            fields = response.json()
            
            field_mappings = {}
            custom_fields = []
            
            for field in fields:
                field_id = field.get('id', '')
                field_name = field.get('name', '')
                field_type = field.get('schema', {}).get('type', 'unknown')
                
                field_mappings[field_name] = field_id
                
                if field_id.startswith('customfield_'):
                    custom_fields.append({
                        'Field_ID': field_id,
                        'Field_Name': field_name,
                        'Field_Type': field_type,
                        'Is_Custom': True
                    })
            
            return field_mappings, custom_fields
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching field mappings: {e}")
            return {}, []
    
    def analyze_issue_custom_fields(self, project_key, max_issues=5):
        """
        Analyze custom fields in actual issues from a project
        
        Args:
            project_key (str): Project key to analyze
            max_issues (int): Maximum number of issues to analyze
            
        Returns:
            list: List of field analysis results
        """
        # Get issues from the project
        jql = f"project = '{project_key}' ORDER BY created DESC"
        
        url = f"{self.jira_url}/rest/api/2/search"
        params = {
            'jql': jql,
            'maxResults': max_issues,
            'fields': 'customfield_*,summary,key,issuetype',
            'expand': 'names'
        }
        
        try:
            response = self.session.get(url, params=params, auth=self.auth)
            response.raise_for_status()
            search_results = response.json()
            
            field_analysis = []
            issues = search_results.get('issues', [])
            names_mapping = search_results.get('names', {})
            
            if not issues:
                print(f"No issues found in project {project_key}")
                return []
            
            print(f"Analyzing {len(issues)} issues from project {project_key}")
            
            # Collect all custom field data
            all_custom_fields = {}
            
            for issue in issues:
                issue_key = issue.get('key', '')
                fields = issue.get('fields', {})
                issue_type = fields.get('issuetype', {}).get('name', 'Unknown')
                summary = fields.get('summary', '')[:50] + '...' if fields.get('summary') else ''
                
                print(f"\nAnalyzing: {issue_key} ({issue_type})")
                print(f"Summary: {summary}")
                
                for field_id, field_value in fields.items():
                    if field_id.startswith('customfield_') and field_value is not None:
                        field_name = names_mapping.get(field_id, 'Unknown Field Name')
                        
                        if field_id not in all_custom_fields:
                            all_custom_fields[field_id] = {
                                'Field_ID': field_id,
                                'Field_Name': field_name,
                                'Sample_Values': [],
                                'Value_Types': set(),
                                'Issues_With_Data': [],
                                'Potential_Purpose': self._guess_field_purpose(field_name, field_value)
                            }
                        
                        # Store sample data
                        value_type = type(field_value).__name__
                        all_custom_fields[field_id]['Value_Types'].add(value_type)
                        all_custom_fields[field_id]['Issues_With_Data'].append(issue_key)
                        
                        # Store sample values (limit to avoid too much data)
                        if len(all_custom_fields[field_id]['Sample_Values']) < 3:
                            sample_value = self._format_sample_value(field_value)
                            all_custom_fields[field_id]['Sample_Values'].append({
                                'Issue': issue_key,
                                'Value': sample_value,
                                'Type': value_type
                            })
            
            # Convert to list for easier handling
            for field_data in all_custom_fields.values():
                field_data['Value_Types'] = list(field_data['Value_Types'])
                field_analysis.append(field_data)
            
            return field_analysis
            
        except requests.exceptions.RequestException as e:
            print(f"Error analyzing issues: {e}")
            return []
    
    def _guess_field_purpose(self, field_name, field_value):
        """Guess the purpose of a custom field based on name and value"""
        field_name_lower = field_name.lower()
        
        # Check for common field purposes
        if any(keyword in field_name_lower for keyword in ['story point', 'point', 'estimate']):
            return 'Story_Points'
        elif any(keyword in field_name_lower for keyword in ['sprint']):
            return 'Sprint'
        elif any(keyword in field_name_lower for keyword in ['sdlc', 'lifecycle', 'environment']):
            return 'SDLC_Information'
        elif any(keyword in field_name_lower for keyword in ['app', 'application']):
            return 'Application_Name'
        elif any(keyword in field_name_lower for keyword in ['acceptance', 'criteria', 'ac']):
            return 'Acceptance_Criteria'
        elif any(keyword in field_name_lower for keyword in ['feature', 'link', 'url']):
            return 'Feature_Link'
        elif any(keyword in field_name_lower for keyword in ['note', 'comment', 'remark']):
            return 'Notes'
        elif isinstance(field_value, str) and ('http' in str(field_value) or 'www' in str(field_value)):
            return 'Possible_Link'
        else:
            return 'Unknown'
    
    def _format_sample_value(self, field_value):
        """Format field value for display"""
        if isinstance(field_value, dict):
            if 'name' in field_value:
                return f"Dict with name: {field_value['name']}"
            elif 'value' in field_value:
                return f"Dict with value: {field_value['value']}"
            else:
                return f"Dict: {str(field_value)[:100]}..."
        elif isinstance(field_value, list):
            if len(field_value) > 0:
                first_item = field_value[0]
                if isinstance(first_item, dict) and 'name' in first_item:
                    return f"List of dicts with names: {[item.get('name', str(item)[:30]) for item in field_value[:2]]}..."
                else:
                    return f"List: {str(field_value)[:100]}..."
            else:
                return "Empty list"
        else:
            return str(field_value)[:100] + ('...' if len(str(field_value)) > 100 else '')
    
    def generate_field_report(self, project_keys, output_file='jira_field_analysis.xlsx'):
        """
        Generate comprehensive field analysis report
        
        Args:
            project_keys (str): Comma-separated project keys
            output_file (str): Output Excel file name
        """
        print("="*80)
        print("JIRA CUSTOM FIELDS ANALYSIS REPORT")
        print("="*80)
        
        # Get all field mappings
        print("\n1. Fetching all field mappings...")
        field_mappings, all_custom_fields = self.get_all_field_mappings()
        print(f"   Found {len(all_custom_fields)} custom fields")
        
        # Analyze issues from each project
        print("\n2. Analyzing issues from projects...")
        project_list = [key.strip() for key in project_keys.split(',')]
        
        all_field_analysis = []
        
        for project_key in project_list:
            print(f"\n   Analyzing project: {project_key}")
            field_analysis = self.analyze_issue_custom_fields(project_key, max_issues=5)
            all_field_analysis.extend(field_analysis)
        
        # Create comprehensive report
        print("\n3. Creating comprehensive report...")
        
        # Combine data
        field_report = []
        processed_fields = set()
        
        for field_data in all_field_analysis:
            field_id = field_data['Field_ID']
            if field_id not in processed_fields:
                processed_fields.add(field_id)
                
                report_row = {
                    'Field_ID': field_id,
                    'Field_Name': field_data['Field_Name'],
                    'Guessed_Purpose': field_data['Potential_Purpose'],
                    'Value_Types': ', '.join(field_data['Value_Types']),
                    'Sample_Issue_Keys': ', '.join(field_data['Issues_With_Data'][:3]),
                    'Sample_Values': '; '.join([f"{sv['Issue']}: {sv['Value']}" for sv in field_data['Sample_Values']]),
                    'Usage_Count': len(field_data['Issues_With_Data'])
                }
                field_report.append(report_row)
        
        # Sort by usage count (most used first)
        field_report.sort(key=lambda x: x['Usage_Count'], reverse=True)
        
        # Create DataFrames
        field_report_df = pd.DataFrame(field_report)
        all_custom_fields_df = pd.DataFrame(all_custom_fields)
        
        # Print summary to console
        print(f"\n{'='*80}")
        print("SUMMARY OF CUSTOM FIELDS FOUND:")
        print(f"{'='*80}")
        
        purpose_groups = {}
        for row in field_report:
            purpose = row['Guessed_Purpose']
            if purpose not in purpose_groups:
                purpose_groups[purpose] = []
            purpose_groups[purpose].append(row)
        
        for purpose, fields in purpose_groups.items():
            print(f"\n🎯 {purpose.upper().replace('_', ' ')}:")
            for field in fields:
                print(f"   {field['Field_ID']} - {field['Field_Name']}")
                if field['Sample_Values']:
                    print(f"      Sample: {field['Sample_Values'][:100]}...")
        
        # Export to Excel
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                field_report_df.to_excel(writer, sheet_name='Field_Analysis', index=False)
                all_custom_fields_df.to_excel(writer, sheet_name='All_Custom_Fields', index=False)
                
                # Auto-adjust column widths
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
                        adjusted_width = min(max_length + 2, 80)
                        worksheet.column_dimensions[column_letter].width = adjusted_width
            
            print(f"\n✅ Field analysis exported to: {output_file}")
            
        except Exception as e:
            print(f"\n❌ Error exporting to Excel: {e}")
        
        print(f"\n{'='*80}")
        print("NEXT STEPS:")
        print("1. Review the Excel file for detailed field analysis")
        print("2. Share the console output above with the developer")
        print("3. The developer will update the main script with correct field IDs")
        print(f"{'='*80}")
        
        return field_report_df, all_custom_fields_df

def main():
    """
    Main function to run field analysis
    """
    # Configuration - UPDATE THESE WITH YOUR DETAILS
    JIRA_URL = "https://your-jira-instance.com"  # Replace with your Jira URL
    USERNAME = "your-username"  # Replace with your username  
    PASSWORD = "your-password"  # Replace with your password/API token
    
    # Project keys to analyze - UPDATE THIS
    PROJECT_KEYS = "PROJ1,PROJ2"  # Replace with your actual project keys (comma-separated)
    
    try:
        print("🔍 JIRA CUSTOM FIELDS ANALYZER")
        print("This will analyze your Jira instance to identify custom field IDs")
        print("="*60)
        
        # Initialize analyzer
        analyzer = JiraFieldAnalyzer(JIRA_URL, USERNAME, PASSWORD)
        
        # Generate comprehensive report
        field_df, custom_df = analyzer.generate_field_report(PROJECT_KEYS)
        
        print("\n🎉 Analysis completed!")
        print("\nPlease share the 'SUMMARY OF CUSTOM FIELDS FOUND' section above")
        print("with the developer to update the main script with correct field IDs.")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")

if __name__ == "__main__":
    main()
