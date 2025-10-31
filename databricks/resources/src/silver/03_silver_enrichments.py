import dlt
from pyspark.sql.functions import col, when, current_timestamp

def create_outcome_categories(df_filtered):
    """
    Creates detailed outcome categories based on resolution descriptions.
    
    Args:
        df_filtered: PySpark DataFrame with resolution_description column
        
    Returns:
        DataFrame with added detailed_outcome_category column
    """
    
    # Define categorization patterns for better maintainability
    patterns = {
        # Duplicate/Already Exists patterns
        "Duplicate Request": r"(?i)(duplicate|already exists|open service request|DSNY already has a request|earlier complaint.*same location)",
        "Duplicate/Already Addressed": r"(?i)(addressed under another|another service request)",
        
        # Vehicle related
        "Vehicle Removed/Claimed": r"(?i)(vehicle.*removed|removed.*vehicle|owner claimed.*vehicle)",
        "Derelict Vehicle Process": r"(?i)(derelict|junk.*vehicle|abandoned vehicle)",
        "Not Abandoned - Owner Claimed": r"(?i)(owner claimed.*bicycle.*not abandoned|claimed.*not abandoned)",
        
        # Legal/Fine related
        "Fine Paid": r"(?i)(pled guilty.*paid.*fine|fine paid)",
        "Hearing Scheduled": r"(?i)(hearing.*scheduled)",
        "Summons/Violation Issued": r"(?i)(summons.*issued|issued.*summons|notice of violation)",
        "Arrest Made": r"(?i)(made.*arrest)",
        "Warning/Counseling Issued": r"(?i)(counseled|warned|warning)",
        "Violation/Hazard Found": r"(?i)(found asbestos|issued.*violation|stop work order)",
        "Violation Issued - DSNY": r"(?i)(DSNY.*gave.*violation|Sanitation.*gave.*business.*violation)",
        
        # Service refusal/acceptance
        "Services Refused/Declined": r"(?i)(refused services|refused.*assistance|mobile outreach.*not accept|services declined)",
        "Services Accepted": r"(?i)(accepted services|person.*accepted)",
        "Request Denied/Refused": r"(?i)(denied.*request|owner refused|past.*notification period)",
        
        # Items/Equipment/Collections
        "Test Kit Sent": r"(?i)(mailed.*test kit|test kit sent)",
        "Items Collected/Removed": r"(?i)(collected.*e-waste|collected.*waste|collected.*items|items.*removed|removed.*items)",
        "Items Collected": r"(?i)(picked up.*items|collected.*items)",
        "Equipment Installed": r"(?i)(placed.*location|installed|basket.*placed|litter basket)",
        "E-Waste/Scooter Service": r"(?i)(e-waste|e-scooter)",
        
        # Work status - Completed
        "Work Completed": r"(?i)(completed.*request|corrected.*condition|performed.*work|removed.*graffiti|fixed|took action to fix|repaired.*problem)",
        "Work Completed - Parks": r"(?i)(Parks.*corrected.*problem|reviewed.*corrected.*problem)",
        "Location Cleaned": r"(?i)(cleaned.*location|location.*cleaned|DSNY.*cleaned.*lot)",
        "Graffiti Service": r"(?i)(graffiti.*removed|graffiti.*scheduled|owner refused.*graffiti)",
        "DOT Repair Completed": r"(?i)(Transportation.*repaired.*bike rack|DOT.*repaired)",
        "Infrastructure Cleaned": r"(?i)(infrastructure.*required cleaning.*completed|DEP.*cleaning.*completed)",
        "Snow Removal - Plowed": r"(?i)(Sanitation.*plowed|DSNY.*plowed|plowed.*area)",
        
        # Work status - Deferred/Future
        "Work Deferred/Seasonal": r"(?i)(deferred|seasonal.*considerations|seasonal.*deferred)",
        "Long Term/Waitlisted": r"(?i)(reconstruction|long term|future project|waitlisted|high.*requests)",
        "Work Order Created": r"(?i)(work order.*filed|processed.*another system)",
        
        # Inspections
        "Inspection Completed": r"(?i)(inspection.*conducted|inspected.*complaint|conducted.*inspection|inspected.*condition|investigated.*complaint)",
        "Pending Inspection": r"(?i)(will inspect|will be inspected|will determine|inspection.*progress)",
        "Inspection Failed": r"(?i)(unable.*inspect|unable.*complete.*inspection|inspection failed)",
        "Passed Inspection/Compliant": r"(?i)(passed inspection|found compliance|in-compliance)",
        "Inspection Decision": r"(?i)(inspection.*warranted|inspection.*not warranted|scheduled.*inspection)",
        "Re-inspection Approved": r"(?i)(approved.*re-inspection|sidewalk re-inspection.*approved)",
        
        # Resource/Access issues
        "Resources Unavailable": r"(?i)(allocated resources.*other|resources unavailable|could not.*resources|devote.*resources|critical services)",
        "Access Denied": r"(?i)(denied access|no.*access|not able.*gain access|unable.*gain entry|could not.*access)",
        
        # Referrals and Coordination
        "Referred/Coordinating": r"(?i)(referred.*to|forwarded|submitted to|coordinating.*agency)",
        "Sent to Appropriate Unit": r"(?i)(sent.*to.*appropriate|sent.*to.*district|re-assignment.*unit)",
        "Referred - No Jurisdiction": r"(?i)(isn't within.*jurisdiction|not.*jurisdiction.*contact)",
        "Referred to Construction Safety": r"(?i)(best addressed by.*Construction Safety|determined.*Construction Safety Enforcement)",
        "Referred to DOT Unit": r"(?i)(jurisdiction.*another.*Transportation unit|another DOT unit|condition.*under.*jurisdiction.*another)",
        "Referred to Other Entity": r"(?i)(another entity.*handle.*condition|notified.*appropriate resource)",
        
        # No violation/issue found
        "No Violation/Problem Found": r"(?i)(did not violate|no violations?|didn't see.*violation|didn't observe|no evidence|did not find.*problem|condition.*not found|no.*found|unable to locate|could not find)",
        "Condition Not Found": r"(?i)(couldn't find.*condition|could not find.*condition|could not locate)",
        "No Action Necessary": r"(?i)(no action.*necessary|no further action|action.*not necessary)",
        "Within Standards/Invalid": r"(?i)(acceptable parameters|within.*parameters|compliance.*standards|complaint.*not warranted|complaint invalid)",
        "DOT - No Issue Found": r"(?i)(DOT.*did not find.*condition|Transportation.*not find.*reported)",
        "Odor Survey - No Issue": r"(?i)(performed.*odor survey.*found no odors|odor survey.*no odors found)",
        
        # Information/Communication
        "Insufficient Information": r"(?i)(insufficient|not.*sufficient|not enough).*(information|location)|(?i)(did not have enough information|insufficient.*information)",
        "Information Provided/Advisory": r"(?i)(website|additional information|check online|provided information|educated|informed|advised|educational outreach)",
        "Report Acknowledged": r"(?i)(thank you.*report|help.*ensure|report.*acknowledged)",
        "See Notes/Info": r"(?i)(see notes|see customer notes)",
        "Letter/Notice Sent": r"(?i)(sent.*letter|sent.*notification|warning letter|letter.*returned)",
        "Report Filed/Submitted": r"(?i)(report.*prepared|report.*submitted|has been submitted)",
        "Graffiti - Owner Notification": r"(?i)(City will notify.*property owner.*graffiti|notify.*owner.*clean.*graffiti)",
        
        # Closed/Resolved status
        "Request Closed/Resolved": r"(?i)(request.*(closed|completed)|administratively closed|SR was.*closed|closed.*complaint|complaint.*closed)",
        "Verified/Phone Resolved": r"(?i)(verified.*corrected|resolved.*phone|resolved.*speaking|resolved.*without inspection)",
        "Subjects Left": r"(?i)(those responsible.*gone|subjects left)",
        "Auto Closed": r"(?i)(auto close|N/A.*SR)",
        
        # Ongoing/Monitoring
        "Follow-up/Monitoring Required": r"(?i)(found violations.*follow-up|still open|conditions.*open|noted.*location|monitor|under observation)",
        "Under Review/Investigation": r"(?i)(under investigation|under review|is reviewing)",
        "Under Surveillance": r"(?i)(note.*location.*surveillance|periodic surveillance)",
        "Scheduled": r"(?i)(has been scheduled|scheduled.*inspection|scheduled.*removal)",
        
        # Utilities and Services
        "Utilities Addressed": r"(?i)(heat.*restored|hot water.*restored|utilities restored|shut.*hydrant)",
        "Regular/Annual Service": r"(?i)(annual|annually|regular schedule|routine)",
        "Assistance Offered": r"(?i)(outreach.*offered|assistance.*offered)",
        
        # Jurisdiction issues
        "No Jurisdiction": r"(?i)(no.*jurisdiction|outside.*jurisdiction|out of.*jurisdiction|not.*jurisdiction)",
        "DSNY No Jurisdiction - Vehicle": r"(?i)(DSNY.*vehicle.*not.*eligible.*removal.*jurisdiction|DSNY.*doesn't have jurisdiction)",
        
        # Resubmit/Additional action required
        "Resubmit Required": r"(?i)(call 311|file a new complaint|submit.*new.*request|refile)",
        "Compliance Deadline Given": r"(?i)(24 hours.*compliance|has.*hours.*come.*compliance)",
        "Immediate Removal Required": r"(?i)(must remove.*items.*immediately|remove.*items.*immediately)",
        
        # Special services and departments
        "Tree Service": r"(?i)(tree.*planting|street tree)",
        "Tree Service - Ineligible": r"(?i)(cannot receive.*tree|ineligible.*tree|conflicts.*infrastructure)",
        "Tree Service - Future Cycle": r"(?i)(pruning cycle|block pruning)",
        "Tree/Sidewalk Repair - Eligible": r"(?i)(meets.*criteria.*repair.*Trees.*Sidewalks|Trees.*Sidewalks Program.*timeframe)",
        "Not Parks Responsibility": r"(?i)(Parks.*not perform.*phone|Parks.*not perform.*cable)",
        
        # Department specific
        "TLC Processing": r"(?i)(TLC.*received|Taxi.*Limousine)",
        "Federal Aviation Issue": r"(?i)(helicopter|FAA|Federal Aviation)",
        "DCWP Review Complete": r"(?i)(DCWP.*finished reviewing|finished reviewing.*complaint.*DCWP)",
        "DCWP Business Inspection": r"(?i)(DCWP.*Enforcement.*inspected.*business|Consumer.*Worker Protection.*inspected)",
        "DEP Report Sent": r"(?i)(Environmental Protection.*sent.*report|DEP.*sent.*report)",
        "Parks to DOT Referral": r"(?i)(Parks.*reviewed.*handled by.*DOT|Parks.*determined.*DOT.*311)",
        "Aging Services - Assisted": r"(?i)(Department.*Aging.*contacted.*provided.*assistance|Aging.*provided.*assistance)",
        "Complaint Inactivated": r"(?i)(notice.*complaint.*inactivation|complaint.*inactivated)",
        "Illegal Posting - Pending Summons": r"(?i)(observed.*illegal postings.*Summons.*responsible party)",
        "DOB Special Operations Assignment": r"(?i)(Buildings.*reviewed.*assigned.*Special Operations)",
        "DHS Mobile Outreach Sent": r"(?i)(Homeless Services.*mobile outreach.*response team)",
        "Department Contact - Resolved": r"(?i)(Department.*Transportation.*contacted.*customer.*resolved|DOT.*contacted.*resolved|Department.*contacted.*resolved)",
        "Reviewed and Assigned": r"(?i)(reviewed.*complaint.*assigned)",
        "DOHMH - Closed": r"(?i)(DOHMH.*actioned.*closed|Health.*Mental Hygiene.*closed)",
        "DOHMH - Under Review": r"(?i)(Health.*Mental Hygiene.*receipt.*complaint.*will review)",
        "Parks - Will Investigate": r"(?i)(Parks.*Recreation.*will visit.*investigate|Parks.*will.*investigate.*condition)",
        "Parks - Pending Funding": r"(?i)(Parks.*Recreation.*inspected.*repair site.*timeframe.*depends.*funding)",
        "Unauthorized Operation - Notified": r"(?i)(establishment.*operating without authorization.*notified.*register)",
        
        # Contact issues
        "Target/Individual Not Found": r"(?i)(no encampment.*found|could not find.*individual|individual not found|unable to find.*item|lost item)",
        "Contact Attempted": r"(?i)(message.*left|could not contact|tried.*could not contact)"
    }
    
    # Build the when chain dynamically
    when_chain = None
    
    for category, pattern in patterns.items():
        condition = col("resolution_description").rlike(pattern)
        
        if when_chain is None:
            when_chain = when(condition, category)
        else:
            when_chain = when_chain.when(condition, category)
    
    # Add the final otherwise clause
    when_chain = when_chain.otherwise("Other")
    
    # Apply the categorization
    df_outcome_categories = df_filtered.withColumn(
        "detailed_outcome_category",
        when_chain
    )
    
    return df_outcome_categories

def create_main_category_mapping(df_outcome_categories):
    """
    Maps detailed outcome categories to main categories using a clean, maintainable structure.
    
    Args:
        df_outcome_categories: DataFrame with detailed_outcome_category column
        
    Returns:
        DataFrame with main_outcome_category column added
    """
    
    # Define category mappings for better maintainability
    category_mappings = {
        "Completed Actions": [
            # Work completions
            "Work Completed", "Work Completed - Parks", "Location Cleaned", 
            "Items Collected/Removed", "Items Collected", "Equipment Installed", 
            "Vehicle Removed/Claimed", "Tree Service", "Graffiti Service",
            "Utilities Addressed",
            # Inspections that resulted in completion
            "Inspection Completed", "Passed Inspection/Compliant"
        ],
        
        "No Issue Found": [
            "No Violation/Problem Found", "No Action Necessary", 
            "Within Standards/Invalid", "Condition Not Found"
        ],
        
        "Violations/Penalties": [
            "Summons/Violation Issued", "Fine Paid", "Hearing Scheduled",
            "Warning/Counseling Issued", "Arrest Made", "Violation/Hazard Found"
        ],
        
        "Information/Advisory": [
            "Information Provided/Advisory", "Report Acknowledged", 
            "Letter/Notice Sent", "Test Kit Sent", "See Notes/Info",
            "Report Filed/Submitted"
        ],
        
        "Jurisdiction/Process Issues": [
            "Access Denied", "No Jurisdiction", "Duplicate Request", 
            "Insufficient Information", "Services Refused/Declined",
            "Request Denied/Refused", "DSNY No Jurisdiction - Vehicle",
            "Not Parks Responsibility"
        ],
        
        "Ongoing/Pending": [
            "Follow-up/Monitoring Required", "Pending Inspection", 
            "Under Review/Investigation", "Scheduled", "Long Term/Waitlisted",
            "Under Surveillance", "Work Deferred/Seasonal"
        ],
        
        "Referral/Coordination": [
            "Referred/Coordinating", "Sent to Appropriate Unit", 
            "Referred - No Jurisdiction", "Parks to DOT Referral"
        ],
        
        "Closed/Resolved": [
            "Request Closed/Resolved", "Subjects Left", 
            "Duplicate/Already Addressed", "Verified/Phone Resolved",
            "Auto Closed"
        ],
        
        "Action Required": [
            "Resubmit Required", "Compliance Deadline Given",
            "Immediate Removal Required"
        ],
        
        "Unable to Complete": [
            "Resources Unavailable", "Contact Attempted",
            "Target/Individual Not Found", "Inspection Failed"
        ],
        
        "Specialized Services": [
            "Tree Service - Ineligible", "Tree Service - Future Cycle",
            "Tree/Sidewalk Repair - Eligible", "Federal Aviation Issue",
            "TLC Processing", "E-Waste/Scooter Service"
        ],
        
        "Department Specific": [
            "DCWP Review Complete", "DCWP Business Inspection", "DEP Report Sent",
            "Aging Services - Assisted", "DHS Mobile Outreach Sent",
            "DOB Special Operations Assignment", "Department Contact - Resolved",
            "Regular/Annual Service"
        ]
    }
    
    # Build the when chain dynamically
    when_chain = None
    
    for main_category, detailed_categories in category_mappings.items():
        condition = col("detailed_outcome_category").isin(detailed_categories)
        
        if when_chain is None:
            when_chain = when(condition, main_category)
        else:
            when_chain = when_chain.when(condition, main_category)
    
    # Add the final otherwise clause
    when_chain = when_chain.otherwise("Other")
    
    # Apply the main category mapping
    df_final = df_outcome_categories.withColumn(
        "main_outcome_category",
        when_chain
    )
    
    return df_final


@dlt.table(
    name="silver_enriched_311", 
    comment="Parse resolution descriptions and derive analytical columns with categorization"
)
def silver_enrichments():
    df = dlt.read_stream("silver_311_unified")
    
    # Apply resolution categorization
    df = create_outcome_categories(df)
    df = create_main_category_mapping(df)
    
    # Follow-up required flag based on resolution text
    df = df.withColumn("requires_follow_up",
        when(col("resolution_description").rlike("(?i)file a new complaint|still exists"), True)
        .when(col("resolution_description").rlike("(?i)keep.*under observation|monitoring"), True)
        .when(col("resolution_description").rlike("(?i)follow-up|will inspect"), True)
        .otherwise(False))
    
    # Violation issued flag
    df = df.withColumn("violation_issued",
        col("resolution_description").rlike("(?i)issued.*violation|issued.*summons|issued.*notice"))
    
    # Was inspected flag (inspection/investigation performed)
    df = df.withColumn("was_inspected",
        col("resolution_description").rlike("(?i)inspected|investigated|inspection.*conducted"))
    
    # Calculate response time in hours (for closed requests)
    df = df.withColumn("response_time_hours",
        when(col("is_closed") == True,
             (col("closed_date").cast("long") - col("created_date").cast("long")) / 3600)
        .otherwise(None))
    
    # Response time in days for easier interpretation
    df = df.withColumn("response_time_days",
        when(col("response_time_hours").isNotNull(),
             col("response_time_hours") / 24)
        .otherwise(None))
    
    # Add SLA category based on response time
    df = df.withColumn("response_sla_category",
        when(col("response_time_hours").isNull(), "STILL_OPEN")
        .when(col("response_time_hours") <= 24, "SAME_DAY")
        .when(col("response_time_hours") <= 168, "WITHIN_WEEK")  # 7 days
        .when(col("response_time_hours") <= 720, "WITHIN_MONTH")  # 30 days
        .otherwise("OVER_MONTH"))
    
    # Processing metadata
    df = df.withColumn("_enrichment_processed_timestamp", current_timestamp())

    # FINAL PROCESSING - Select and order columns
    final_columns = [
        # Core identifiers and dates
        "unique_key", "created_date", "closed_date", 
        
        # Agency and location
        "agency", "agency_name", "borough", "city",
        
        # Complaint details
        "complaint_type", "descriptor", "status",
        
        # Address information
        "incident_address", "street_name", "cross_street_1", "cross_street_2",
        "intersection_street_1", "intersection_street_2", "landmark",
        "location_type", "community_board", "incident_zip",
        
        # Geographic coordinates
        "latitude", "longitude", "has_valid_location",
        
        # Resolution information
        "resolution_description", "resolution_action_updated_date",
        "detailed_outcome_category", "main_outcome_category",
        
        # Analysis flags and metrics
        "was_inspected", "violation_issued", "requires_follow_up",
        "has_resolution", "is_closed",
        
        # Response time metrics
        "response_time_hours", "response_time_days", "response_sla_category",
        
        # Temporal dimensions
        "created_year", "created_month", "created_dayofweek", "created_hour",
        "closed_year", "closed_month",
        
        # Temporal flags
        "is_business_hours", "is_weekend",
        
        # Address classification
        "address_type"
    ]
    
    # Column validation
    missing_cols = set(final_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")
    
    # Final selection and processing timestamp
    df = df.select(*final_columns) \
           .withColumn("_processed_timestamp", current_timestamp())
    
    return df
    
    