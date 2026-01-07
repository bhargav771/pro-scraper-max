"""
Hunter.io API integration to find owner name and email
"""
import requests
import logging

def find_owner_email_hunter(domain, api_key):
    """
    Find owner/decision maker email using Hunter.io API
    Returns dict with top 4 owner emails
    """
    if not domain or not api_key:
        return {
            'owner_name': '', 'owner_email': '', 'owner_position': '', 'confidence': 0,
            'owner_name_2': '', 'owner_email_2': '', 'owner_position_2': '', 'confidence_2': 0,
            'owner_name_3': '', 'owner_email_3': '', 'owner_position_3': '', 'confidence_3': 0,
            'owner_name_4': '', 'owner_email_4': '', 'owner_position_4': '', 'confidence_4': 0
        }
    
    try:
        # Clean domain
        domain = domain.replace('http://', '').replace('https://', '').replace('www.', '').split('/')[0].split('?')[0]
        
        print(f"[Hunter.io] Searching domain: {domain}")
        
        # Hunter.io Domain Search API
        url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={api_key}&limit=10"
        response = requests.get(url, timeout=15)
        
        print(f"[Hunter.io] Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            print(f"[Hunter.io] Response data: {data.get('data', {}).get('emails', []).__len__()} emails found")
            
            if data.get('data') and data['data'].get('emails'):
                emails = data['data']['emails']
                
                print(f"[Hunter.io] Raw emails data: {emails[:2]}")  # Print first 2 emails
                
                # Priority: owner, ceo, founder, director, manager
                priority_titles = ['owner', 'ceo', 'founder', 'co-founder', 'director', 'manager', 'president']
                
                # Sort by priority
                priority_emails = []
                other_emails = []
                
                for email_data in emails:
                    position = (email_data.get('position') or '').lower()
                    is_priority = any(title in position for title in priority_titles)
                    
                    print(f"[Hunter.io] Email: {email_data.get('value')} | Position: {position} | Priority: {is_priority}")
                    
                    if is_priority:
                        priority_emails.append(email_data)
                    else:
                        other_emails.append(email_data)
                
                print(f"[Hunter.io] Priority emails: {len(priority_emails)}, Other emails: {len(other_emails)}")
                
                # Combine: priority first, then others
                all_emails = priority_emails + other_emails
                
                # Get top 4
                result = {
                    'owner_name': '', 'owner_email': '', 'owner_position': '', 'confidence': 0,
                    'owner_name_2': '', 'owner_email_2': '', 'owner_position_2': '', 'confidence_2': 0,
                    'owner_name_3': '', 'owner_email_3': '', 'owner_position_3': '', 'confidence_3': 0,
                    'owner_name_4': '', 'owner_email_4': '', 'owner_position_4': '', 'confidence_4': 0
                }
                
                for i, email_data in enumerate(all_emails[:4]):
                    suffix = '' if i == 0 else f'_{i+1}'
                    name = f"{email_data.get('first_name', '')} {email_data.get('last_name', '')}".strip()
                    email = email_data.get('value', '')
                    position = email_data.get('position', '')
                    conf = email_data.get('confidence', 0)
                    
                    result[f'owner_name{suffix}'] = name
                    result[f'owner_email{suffix}'] = email
                    result[f'owner_position{suffix}'] = position
                    result[f'confidence{suffix}'] = conf
                    
                    print(f"[Hunter.io] Option {i+1}: {name} | {email} | {position} | {conf}%")
                
                print(f"[Hunter.io] Found {len(all_emails[:4])} emails")
                return result
        else:
            print(f"[Hunter.io] API Error: {response.status_code} - {response.text[:200]}")
        
        return {
            'owner_name': '', 'owner_email': '', 'owner_position': '', 'confidence': 0,
            'owner_name_2': '', 'owner_email_2': '', 'owner_position_2': '', 'confidence_2': 0,
            'owner_name_3': '', 'owner_email_3': '', 'owner_position_3': '', 'confidence_3': 0,
            'owner_name_4': '', 'owner_email_4': '', 'owner_position_4': '', 'confidence_4': 0
        }
    
    except Exception as e:
        print(f"[Hunter.io] Exception: {str(e)}")
        logging.error(f"Hunter.io API error: {str(e)}")
        return {
            'owner_name': '', 'owner_email': '', 'owner_position': '', 'confidence': 0,
            'owner_name_2': '', 'owner_email_2': '', 'owner_position_2': '', 'confidence_2': 0,
            'owner_name_3': '', 'owner_email_3': '', 'owner_position_3': '', 'confidence_3': 0,
            'owner_name_4': '', 'owner_email_4': '', 'owner_position_4': '', 'confidence_4': 0
        }


def find_email_by_name(domain, first_name, last_name, api_key):
    """
    Find email for specific person using Hunter.io Email Finder
    """
    if not domain or not api_key or not first_name:
        return ''
    
    try:
        domain = domain.replace('http://', '').replace('https://', '').replace('www.', '').split('/')[0]
        
        url = f"https://api.hunter.io/v2/email-finder?domain={domain}&first_name={first_name}&last_name={last_name}&api_key={api_key}"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('data') and data['data'].get('email'):
                return data['data']['email']
        
        return ''
    
    except Exception as e:
        logging.error(f"Hunter.io Email Finder error: {str(e)[:50]}")
        return ''
