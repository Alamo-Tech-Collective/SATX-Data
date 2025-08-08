# US-Based Crime Severity Weights using Federal Sentencing Guidelines
# Based on US Sentencing Commission Guidelines Manual
# Weights represent days of imprisonment based on offense levels

# Federal Offense Level to Days conversion (midpoint of sentencing range)
# Level 43 = Life imprisonment (using 30 years as proxy)
# Lower levels use actual sentencing table midpoints

US_CRIME_WEIGHTS = {
    # Tier 1: Most Severe (Level 35-43) - Life/Death eligible
    'MURDER': 10950,  # Level 43 (life)
    'CAPITAL MURDER': 10950,  # Level 43 (death/life)
    'MURDER 1ST DEGREE': 10950,
    'MURDER 2ND DEGREE': 7300,  # Level 38
    'MANSLAUGHTER': 2920,  # Level 29
    
    # Sexual Offenses - Federal levels vary by victim age and circumstances
    'SEXUAL ASSAULT OF A CHILD': 5475,  # Level 34
    'AGGRAVATED SEXUAL ASSAULT': 4380,  # Level 32
    'SEXUAL ASSAULT': 2920,  # Level 27
    'RAPE': 2920,  # Level 27
    'INDECENT EXPOSURE': 180,  # Level 12
    
    # Kidnapping/Abduction - Level varies by circumstances
    'KIDNAPPING': 4745,  # Level 32
    'AGGRAVATED KIDNAPPING': 5475,  # Level 34
    'FALSE IMPRISONMENT': 730,  # Level 18
    
    # Robbery - Federal levels distinguish by weapon use
    'AGGRAVATED ROBBERY': 3285,  # Level 27 (with weapon)
    'ROBBERY': 2190,  # Level 23 (without weapon)
    'CARJACKING': 3650,  # Level 28
    
    # Assault - Wide range based on injury and weapon
    'AGGRAVATED ASSAULT': 1460,  # Level 24
    'ASSAULT WITH DEADLY WEAPON': 1825,  # Level 25
    'ASSAULT CAUSING BODILY INJURY': 730,  # Level 18
    'ASSAULT': 365,  # Level 14
    'SIMPLE ASSAULT': 180,  # Level 12
    'FAMILY VIOLENCE': 365,  # Level 14
    
    # Arson - Federal level based on risk to life
    'ARSON': 1825,  # Level 24 (structure)
    'ARSON OF VEHICLE': 730,  # Level 18
    
    # Burglary - Federal levels distinguish dwelling vs commercial
    'BURGLARY OF HABITATION': 1095,  # Level 20 (dwelling)
    'BURGLARY OF BUILDING': 730,  # Level 17 (commercial)
    'BURGLARY OF VEHICLE': 365,  # Level 12
    
    # Theft/Larceny - Based on loss amount (using $50k as baseline)
    'AUTO THEFT': 730,  # Level 18
    'THEFT': 180,  # Level 12 (general)
    'SHOPLIFTING': 90,  # Level 8
    'THEFT FROM MOTOR VEHICLE': 180,  # Level 12
    'THEFT OF SERVICE': 90,  # Level 8
    'IDENTITY THEFT': 730,  # Level 18
    
    # Fraud - Federal levels based on loss amount
    'FRAUD': 365,  # Level 14
    'CREDIT CARD FRAUD': 365,  # Level 14
    'FORGERY': 365,  # Level 14
    'COUNTERFEITING': 730,  # Level 18
    
    # Drug Offenses - Federal levels based on type and quantity
    'DRUG TRAFFICKING': 1460,  # Level 24 (major)
    'DRUG DISTRIBUTION': 730,  # Level 18
    'DRUG POSSESSION': 90,  # Level 8 (simple possession)
    'DRUG PARAPHERNALIA': 30,  # Level 6
    
    # Weapons Offenses
    'WEAPON LAW VIOLATIONS': 365,  # Level 14
    'CARRYING CONCEALED WEAPON': 180,  # Level 12
    'POSSESSION OF PROHIBITED WEAPON': 730,  # Level 18
    
    # DUI/DWI - State offense, using typical sentences
    'DWI': 180,  # Level 12
    'DUI': 180,  # Level 12
    
    # Property Damage
    'CRIMINAL MISCHIEF': 90,  # Level 8
    'VANDALISM': 90,  # Level 8
    'GRAFFITI': 30,  # Level 6
    
    # Public Order
    'DISORDERLY CONDUCT': 30,  # Level 6
    'PUBLIC INTOXICATION': 7,  # Level 4
    'CRIMINAL TRESPASS': 30,  # Level 6
    'EVADING ARREST': 180,  # Level 12
    'RESISTING ARREST': 90,  # Level 8
    
    # Other
    'PROSTITUTION': 30,  # Level 6
    'GAMBLING': 30,  # Level 6
    'CITY ORDINANCE VIOLATIONS': 7,  # Level 4
    'TRAFFIC VIOLATIONS': 7,  # Level 4
}

def get_us_crime_weight(crime_type):
    """
    Get the severity weight for a specific crime type based on Federal Sentencing Guidelines.
    Returns a default weight if crime type not found.
    """
    # Convert to uppercase for matching
    crime_upper = crime_type.upper().strip()
    
    # Direct match
    if crime_upper in US_CRIME_WEIGHTS:
        return US_CRIME_WEIGHTS[crime_upper]
    
    # Partial matching for common patterns
    for key, weight in US_CRIME_WEIGHTS.items():
        if key in crime_upper or crime_upper in key:
            return weight
    
    # Check for keywords to assign default weights
    if any(word in crime_upper for word in ['MURDER', 'HOMICIDE']):
        return 7300  # Default to 2nd degree murder
    elif any(word in crime_upper for word in ['SEXUAL', 'RAPE']):
        return 2920  # Sexual assault baseline
    elif any(word in crime_upper for word in ['ASSAULT', 'BATTERY']):
        return 365  # Simple assault baseline
    elif any(word in crime_upper for word in ['ROBBERY', 'MUGGING']):
        return 2190  # Robbery baseline
    elif any(word in crime_upper for word in ['BURGLARY', 'BREAKING']):
        return 730  # Burglary baseline
    elif any(word in crime_upper for word in ['THEFT', 'LARCENY', 'STEALING']):
        return 180  # Theft baseline
    elif any(word in crime_upper for word in ['DRUG', 'NARCOTIC', 'SUBSTANCE']):
        return 365  # Drug offense baseline
    elif any(word in crime_upper for word in ['FRAUD', 'FORGERY', 'EMBEZZLEMENT']):
        return 365  # Fraud baseline
    elif any(word in crime_upper for word in ['VANDALISM', 'GRAFFITI', 'MISCHIEF']):
        return 90  # Property damage baseline
    elif any(word in crime_upper for word in ['WEAPON', 'FIREARM', 'GUN']):
        return 365  # Weapons violation baseline
    
    # Default weight for unmatched crimes (Level 8)
    return 90

def calculate_violent_crime_multiplier(crime_against):
    """
    Additional multiplier for crimes against persons following federal guidelines.
    Federal guidelines often enhance sentences for crimes against persons.
    """
    if crime_against and crime_against.upper() == 'PERSON':
        return 1.25  # 25% enhancement for crimes against persons
    return 1.0

def get_us_weighted_severity(crime_type, crime_against=None):
    """
    Calculate the final weighted severity score using US Federal Sentencing Guidelines.
    """
    base_weight = get_us_crime_weight(crime_type)
    person_multiplier = calculate_violent_crime_multiplier(crime_against)
    return int(base_weight * person_multiplier)