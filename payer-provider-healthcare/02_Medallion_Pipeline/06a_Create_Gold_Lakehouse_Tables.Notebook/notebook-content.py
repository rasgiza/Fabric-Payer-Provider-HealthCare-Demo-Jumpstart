# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a1000001-0001-0001-0001-000000000004",
# META       "default_lakehouse_name": "lh_gold_curated",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "a1000001-0001-0001-0001-000000000004",
# META           "displayName": "lh_gold_curated",
# META           "isDefault": true
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🏆 Create Gold Lakehouse Tables
# 
# **Run this notebook ONCE to create empty Delta tables in `lh_gold_curated`**
# 
# ## Architecture
# ```
# ┌─────────────────────────────────────────────────────────────────────┐
# │                    COMPUTE (T-SQL Stored Procs)                     │
# │                      wh_gold_analytics                              │
# │   ┌─────────────────┐         ┌─────────────────┐                   │
# │   │ usp_Merge_Dim_* │  ───►   │  lh_gold_curated │                  │
# │   │ usp_Merge_Fact_*│  WRITE  │  (Lakehouse)     │                  │
# │   └─────────────────┘   TO    └─────────────────┘                   │
# └─────────────────────────────────────────────────────────────────────┘
#                                         │
#                                         ▼ (Direct Lake)
#                               ┌─────────────────────┐
#                               │   Power BI Report   │
#                               │   (Fastest Mode!)   │
#                               └─────────────────────┘
# ```
# 
# ## ⚠️ Prerequisites
# 1. Create a lakehouse named: `lh_gold_curated`
# 2. Set it as the default lakehouse for this notebook

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
print(f"Creating Gold Lakehouse tables at: {datetime.now()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. ETL Process Log

# CELL ********************

# Process log table for monitoring ETL
spark.sql("""
    CREATE TABLE IF NOT EXISTS etl_process_log (
        process_name STRING,
        status STRING,
        records_processed INT,
        error_message STRING,
        log_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ etl_process_log created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Dimension Tables

# CELL ********************

# dim_date
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INT,
        full_date DATE,
        day_of_week INT,
        day_name STRING,
        day_of_month INT,
        day_of_year INT,
        week_of_year INT,
        month_number INT,
        month_name STRING,
        quarter INT,
        year INT,
        is_weekend INT,
        is_holiday INT,
        fiscal_year INT,
        fiscal_quarter INT
    )
    USING DELTA
""")
print("✓ dim_date created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_patient (SCD Type 2)
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_patient (
        patient_key BIGINT,
        patient_id STRING,
        first_name STRING,
        last_name STRING,
        date_of_birth DATE,
        gender STRING,
        age INT,
        age_group STRING,
        address STRING,
        city STRING,
        state STRING,
        zip_code STRING,
        phone STRING,
        email STRING,
        insurance_type STRING,
        insurance_provider STRING,
        insurance_policy_number STRING,
        -- SCD Type 2 columns
        effective_start_date TIMESTAMP,
        effective_end_date TIMESTAMP,
        is_current INT,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ dim_patient created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_provider (SCD Type 2)
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_provider (
        provider_key BIGINT,
        provider_id STRING,
        first_name STRING,
        last_name STRING,
        display_name STRING,
        npi_number STRING,
        specialty STRING,
        department STRING,
        facility_id STRING,
        is_active INT,
        -- SCD Type 2 columns
        effective_start_date TIMESTAMP,
        effective_end_date TIMESTAMP,
        is_current INT,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ dim_provider created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_payer
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_payer (
        payer_key BIGINT,
        payer_id STRING,
        payer_name STRING,
        payer_type STRING,
        is_active INT,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ dim_payer created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_facility
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_facility (
        facility_key BIGINT,
        facility_id STRING,
        facility_name STRING,
        facility_type STRING,
        city STRING,
        state STRING,
        bed_count INT,
        is_active INT,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ dim_facility created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_monitor (Ontology PatientMonitor entity - static device data)
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_monitor (
        monitor_key BIGINT,
        device_id STRING,
        device_type STRING,
        device_model STRING,
        location_id STRING,
        unit_name STRING,
        floor_number INT,
        building STRING,
        facility_id STRING,
        -- Surrogate FK columns for ontology relationships
        patient_key BIGINT,
        facility_key BIGINT,
        location_key BIGINT,
        is_active INT,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ dim_monitor created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_medication (Type 1 - RxNorm reference data)
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_medication (
        medication_key BIGINT,
        rxnorm_code STRING,
        medication_name STRING,
        generic_name STRING,
        drug_class STRING,
        therapeutic_area STRING,
        route STRING,
        form STRING,
        strength STRING,
        is_chronic INT,
        is_active INT,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ dim_medication created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_diagnosis (Type 1 - ICD-10 code reference)
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_diagnosis (
        diagnosis_key BIGINT,
        icd_code STRING,
        icd_description STRING,
        icd_category STRING,
        is_chronic INT,
        is_active INT,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ dim_diagnosis created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_sdoh (Type 1 - Social Determinants of Health by zip code)
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_sdoh (
        sdoh_key BIGINT,
        zip_code STRING,
        state STRING,
        poverty_rate DOUBLE,
        food_desert_flag INT,
        transportation_score DOUBLE,
        housing_instability_rate DOUBLE,
        uninsured_rate DOUBLE,
        broadband_access_pct DOUBLE,
        median_household_income INT,
        population INT,
        social_vulnerability_index DOUBLE,
        risk_tier STRING,
        is_active INT,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ dim_sdoh created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Fact Tables

# CELL ********************

# fact_encounter
spark.sql("""
    CREATE TABLE IF NOT EXISTS fact_encounter (
        encounter_key BIGINT,
        encounter_id STRING,
        encounter_date_key INT,
        discharge_date_key INT,
        patient_key BIGINT,
        provider_key BIGINT,
        facility_key BIGINT,
        encounter_type STRING,
        admission_type STRING,
        discharge_disposition STRING,
        length_of_stay INT,
        total_charges DOUBLE,
        total_cost DOUBLE,
        readmission_flag INT,
        -- ML Predictions
        readmission_risk_score DOUBLE,
        readmission_risk_category STRING,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ fact_encounter created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact_claim
spark.sql("""
    CREATE TABLE IF NOT EXISTS fact_claim (
        claim_key BIGINT,
        claim_id STRING,
        claim_date_key INT,
        payment_date_key INT,
        patient_key BIGINT,
        provider_key BIGINT,
        payer_key BIGINT,
        encounter_key BIGINT,
        facility_key BIGINT,
        claim_type STRING,
        claim_status STRING,
        billed_amount DOUBLE,
        allowed_amount DOUBLE,
        paid_amount DOUBLE,
        denial_flag INT,
        -- ML Predictions
        denial_risk_score DOUBLE,
        denial_risk_category STRING,
        primary_denial_reason STRING,
        recommended_action STRING,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ fact_claim created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact_prescription (medication fills with adherence tracking)
spark.sql("""
    CREATE TABLE IF NOT EXISTS fact_prescription (
        prescription_key BIGINT,
        prescription_id STRING,
        prescription_group_id STRING,
        fill_date_key INT,
        patient_key BIGINT,
        provider_key BIGINT,
        payer_key BIGINT,
        encounter_key BIGINT,
        medication_key BIGINT,
        facility_key BIGINT,
        fill_number INT,
        days_supply INT,
        quantity_dispensed INT,
        refills_authorized INT,
        is_generic INT,
        is_chronic_medication INT,
        pharmacy_type STRING,
        total_cost DOUBLE,
        payer_paid DOUBLE,
        patient_copay DOUBLE,
        prescribing_reason_code STRING,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ fact_prescription created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact_diagnosis (encounter → ICD code linkage)
spark.sql("""
    CREATE TABLE IF NOT EXISTS fact_diagnosis (
        fact_diagnosis_key BIGINT,
        diagnosis_id STRING,
        encounter_key BIGINT,
        diagnosis_date_key INT,
        patient_key BIGINT,
        diagnosis_key BIGINT,
        facility_key BIGINT,
        icd_code STRING,
        diagnosis_sequence INT,
        diagnosis_type STRING,
        present_on_admission STRING,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ fact_diagnosis created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Aggregate Tables

# CELL ********************

# agg_readmission_by_date
spark.sql("""
    CREATE TABLE IF NOT EXISTS agg_readmission_by_date (
        encounter_date_key INT,
        encounter_type STRING,
        total_encounters INT,
        high_risk_count INT,
        medium_risk_count INT,
        low_risk_count INT,
        avg_risk_score DOUBLE,
        actual_readmissions INT,
        total_charges DOUBLE,
        avg_length_of_stay DOUBLE
    )
    USING DELTA
""")
print("✓ agg_readmission_by_date created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# agg_denial_by_date
spark.sql("""
    CREATE TABLE IF NOT EXISTS agg_denial_by_date (
        claim_date_key INT,
        claim_type STRING,
        total_claims INT,
        high_risk_count INT,
        medium_risk_count INT,
        low_risk_count INT,
        avg_risk_score DOUBLE,
        actual_denials INT,
        total_billed DOUBLE,
        total_paid DOUBLE,
        at_risk_amount DOUBLE
    )
    USING DELTA
""")
print("✓ agg_denial_by_date created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# agg_medication_adherence (PDC by patient + medication)
spark.sql("""
    CREATE TABLE IF NOT EXISTS agg_medication_adherence (
        adherence_key BIGINT,
        patient_key BIGINT,
        medication_key BIGINT,
        drug_class STRING,
        therapeutic_area STRING,
        measurement_period_start INT,
        measurement_period_end INT,
        total_fills INT,
        total_days_supply INT,
        days_in_period INT,
        covered_days INT,
        pdc_score DOUBLE,
        adherence_category STRING,
        total_medication_cost DOUBLE,
        total_payer_cost DOUBLE,
        total_patient_cost DOUBLE,
        gap_days INT,
        is_chronic INT,
        _load_timestamp TIMESTAMP
    )
    USING DELTA
""")
print("✓ agg_medication_adherence created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4b. Streaming Tables (Vitals / Alerts)

# CELL ********************

# dim_location (Hospital location dimension for streaming data)
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_location (
        location_key BIGINT,
        location_id STRING,
        unit STRING,
        floor INT,
        building STRING,
        unit_type STRING,
        is_critical_care INT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
""")
print("✓ dim_location created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact_vitals (Hourly aggregated vital sign readings)
spark.sql("""
    CREATE TABLE IF NOT EXISTS fact_vitals (
        vitals_key BIGINT,
        patient_key BIGINT,
        date_key INT,
        hour_of_day INT,
        location_key BIGINT,
        avg_heart_rate DOUBLE,
        max_heart_rate INT,
        min_heart_rate INT,
        avg_bp_systolic DOUBLE,
        max_bp_systolic INT,
        avg_bp_diastolic DOUBLE,
        avg_spo2 DOUBLE,
        min_spo2 INT,
        avg_temperature DOUBLE,
        max_temperature DOUBLE,
        avg_respiratory_rate DOUBLE,
        avg_risk_score DOUBLE,
        max_risk_score INT,
        risk_flag STRING,
        critical_reading_count INT,
        reading_count INT,
        created_at TIMESTAMP
    )
    USING DELTA
""")
print("✓ fact_vitals created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact_alerts (Clinical alert records from streaming vitals)
spark.sql("""
    CREATE TABLE IF NOT EXISTS fact_alerts (
        alert_key BIGINT,
        alert_id STRING,
        vitals_id STRING,
        patient_key BIGINT,
        date_key INT,
        alert_hour INT,
        alert_datetime TIMESTAMP,
        location_key BIGINT,
        alert_code STRING,
        alert_description STRING,
        severity STRING,
        severity_rank INT,
        measured_value DOUBLE,
        threshold_value STRING,
        was_acknowledged INT,
        acknowledged_by STRING,
        acknowledged_at TIMESTAMP,
        response_time_minutes DOUBLE,
        created_at TIMESTAMP
    )
    USING DELTA
""")
print("✓ fact_alerts created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4c. Table & Column Descriptions (Data Agent grounding)
#
# Applies business descriptions to every Gold table and its key columns. These
# comments are the metadata the **Fabric Data Agent** reads to understand the
# star schema (grain, join keys, SCD2 `is_current`, and friendly value enums),
# so it generates correct SQL and grounds answers in the right tables.
#
# Idempotent: uses `COMMENT ON TABLE` / `ALTER TABLE ... ALTER COLUMN ... COMMENT`,
# so it both stamps freshly-created tables and updates descriptions on re-runs.

# CELL ********************

# ---------------------------------------------------------------------------
# Apply table + column descriptions (idempotent — safe to re-run).
# Wording is aligned with the HealthcareDemoHLS semantic model so the lakehouse
# and the model tell the agent the same story.
# ---------------------------------------------------------------------------

def _esc(s):
    """Escape single quotes for safe inline SQL string literals."""
    return s.replace("'", "''")

TABLE_COMMENTS = {
    "dim_date": "Calendar date dimension. Grain: one row per calendar day, keyed by date_key (yyyymmdd integer). Join to fact tables on their *_date_key columns. Includes fiscal calendar plus weekend and holiday flags.",
    "dim_patient": "Patient master dimension with SCD Type 2 history. Grain: one row per patient version — always filter is_current = 1 for the latest record. Keyed by patient_key (surrogate); patient_id is the natural key. Contains demographics, geography (zip_code links to dim_sdoh), and insurance attributes.",
    "dim_provider": "Provider (clinician) dimension with SCD Type 2 history. Grain: one row per provider version — filter is_current = 1 for the current record. Keyed by provider_key; provider_id and npi_number are natural keys. Includes specialty, department, and home facility.",
    "dim_payer": "Payer (insurance plan) dimension. Grain: one row per payer. Keyed by payer_key; payer_id is the natural key. payer_type values include Commercial, Medicare, and Medicaid. Used to analyze denials and reimbursement by plan.",
    "dim_facility": "Facility (hospital/clinic) dimension. Grain: one row per facility. Keyed by facility_key; facility_id is the natural key. Includes facility_type, city/state, and bed_count.",
    "dim_monitor": "Patient-monitoring device dimension sourced from the ontology PatientMonitor entity. Grain: one row per device. Keyed by monitor_key; device_id is the natural key. Carries surrogate FKs (patient_key, facility_key, location_key) for graph/ontology relationships.",
    "dim_medication": "Medication reference dimension (RxNorm). Grain: one row per medication. Keyed by medication_key; rxnorm_code is the natural key. drug_class and therapeutic_area support adherence analysis; is_chronic flags maintenance medications.",
    "dim_diagnosis": "Diagnosis reference dimension (ICD-10). Grain: one row per ICD-10 code. Keyed by diagnosis_key; icd_code is the natural key. icd_description is human-readable; is_chronic flags chronic conditions.",
    "dim_sdoh": "Social Determinants of Health dimension at ZIP-code grain. Grain: one row per zip_code. Keyed by sdoh_key. Join to dim_patient on zip_code. Includes poverty_rate, food_desert_flag, social_vulnerability_index, and a derived risk_tier for health-equity analysis.",
    "dim_location": "Hospital location dimension for real-time vitals and alerts. Grain: one row per care location (unit/floor/building). Keyed by location_key; is_critical_care flags ICU-type units.",
    "fact_encounter": "Encounter-level fact for clinical and cost analytics. Grain: one row per patient encounter, keyed by encounter_key. Join to dim_patient (patient_key), dim_provider (provider_key), dim_facility (facility_key), and dim_date (encounter_date_key). Carries ML readmission predictions (readmission_risk_score 0-1, readmission_risk_category High/Medium/Low).",
    "fact_claim": "Claim-level fact for revenue-cycle and denial analytics. Grain: one row per claim, keyed by claim_key. Join to dim_patient, dim_provider, dim_payer, dim_facility, dim_date (claim_date_key), and fact_encounter (encounter_key). Carries ML denial predictions (denial_flag, denial_risk_score 0-1, denial_risk_category, primary_denial_reason).",
    "fact_prescription": "Prescription-fill fact for pharmacy and adherence analytics. Grain: one row per medication fill, keyed by prescription_key. Join to dim_patient, dim_medication, dim_payer, dim_provider, and dim_date (fill_date_key). Cost is split across total_cost, payer_paid, and patient_copay.",
    "fact_diagnosis": "Encounter-to-diagnosis bridge fact. Grain: one row per diagnosis assigned to an encounter, keyed by fact_diagnosis_key. Join to fact_encounter (encounter_key), dim_diagnosis (diagnosis_key), and dim_patient. diagnosis_sequence = 1 marks the primary diagnosis.",
    "fact_vitals": "Hourly aggregated patient vital-sign readings from real-time monitoring. Grain: one row per patient per hour per location. Join to dim_patient, dim_location, and dim_date. risk_flag and critical_reading_count support early-warning analytics.",
    "fact_alerts": "Clinical alert fact generated from streaming vitals. Grain: one row per alert, keyed by alert_key. Join to dim_patient, dim_location, and dim_date. Tracks severity, acknowledgement, and response_time_minutes.",
    "agg_readmission_by_date": "Pre-aggregated readmission metrics. Grain: one row per encounter_date_key and encounter_type. Use for readmission rate and risk trends without scanning fact_encounter.",
    "agg_denial_by_date": "Pre-aggregated claim-denial metrics. Grain: one row per claim_date_key and claim_type. Provides denial counts, at_risk_amount, and risk distribution without scanning fact_claim.",
    "agg_medication_adherence": "Pre-aggregated medication adherence (PDC). Grain: one row per patient_key and medication_key over a measurement period. pdc_score is Proportion of Days Covered (0-1); adherence_category is Adherent/Partial/Non-Adherent. Use for adherence questions instead of recomputing from fact_prescription.",
    "etl_process_log": "Operational ETL audit log. Grain: one row per process execution. Records process_name, status, records_processed, and error_message for pipeline monitoring.",
}

COLUMN_COMMENTS = {
    "dim_patient": {
        "patient_key": "Surrogate primary key. Join target for *_key columns in fact tables.",
        "patient_id": "Natural patient identifier (business key).",
        "age_group": "Banded age group (e.g. 0-17, 18-34, 35-49, 50-64, 65+).",
        "zip_code": "Patient ZIP code; join to dim_sdoh for social-determinant attributes.",
        "insurance_type": "High-level coverage type (Commercial, Medicare, Medicaid, etc.).",
        "is_current": "SCD2 flag — 1 = current version of the patient record. Always filter is_current = 1 for latest values.",
    },
    "dim_provider": {
        "provider_key": "Surrogate primary key. Join target for provider_key in fact tables.",
        "provider_id": "Natural provider identifier (business key).",
        "npi_number": "National Provider Identifier.",
        "specialty": "Provider clinical specialty.",
        "is_current": "SCD2 flag — 1 = current version of the provider record.",
    },
    "dim_payer": {
        "payer_key": "Surrogate primary key. Join target for payer_key in fact_claim/fact_prescription.",
        "payer_name": "Insurance plan / payer name.",
        "payer_type": "Payer category: Commercial, Medicare, or Medicaid.",
    },
    "dim_sdoh": {
        "zip_code": "ZIP code; join key to dim_patient.zip_code.",
        "poverty_rate": "Share of the ZIP population below the poverty line (0-1).",
        "food_desert_flag": "1 if the ZIP is designated a food desert.",
        "social_vulnerability_index": "CDC-style Social Vulnerability Index (0-1, higher = more vulnerable).",
        "risk_tier": "Derived SDOH risk tier (e.g. Low/Medium/High) used for health-equity segmentation.",
    },
    "dim_medication": {
        "medication_key": "Surrogate primary key. Join target for medication_key.",
        "rxnorm_code": "RxNorm concept code (natural key).",
        "drug_class": "Therapeutic drug class.",
        "is_chronic": "1 if the medication is for a chronic/maintenance condition.",
    },
    "dim_diagnosis": {
        "diagnosis_key": "Surrogate primary key. Join target for diagnosis_key.",
        "icd_code": "ICD-10 diagnosis code (natural key).",
        "icd_description": "Human-readable ICD-10 description.",
        "is_chronic": "1 if the diagnosis represents a chronic condition.",
    },
    "fact_encounter": {
        "encounter_key": "Surrogate primary key of the encounter.",
        "encounter_date_key": "Date key of the encounter; join to dim_date.date_key.",
        "patient_key": "FK to dim_patient.",
        "provider_key": "FK to dim_provider.",
        "facility_key": "FK to dim_facility.",
        "encounter_type": "Encounter setting: Inpatient, Outpatient, or Emergency.",
        "length_of_stay": "Inpatient length of stay in days.",
        "total_charges": "Gross charges billed for the encounter (USD).",
        "total_cost": "Cost incurred for the encounter (USD).",
        "readmission_flag": "1 if this encounter was a readmission.",
        "readmission_risk_score": "ML-predicted 30-day readmission probability (0-1).",
        "readmission_risk_category": "Banded readmission risk: High, Medium, or Low.",
    },
    "fact_claim": {
        "claim_key": "Surrogate primary key of the claim.",
        "claim_date_key": "Date the claim was submitted; join to dim_date.date_key.",
        "payment_date_key": "Date the claim was paid; inactive relationship to dim_date.",
        "payer_key": "FK to dim_payer.",
        "encounter_key": "FK to fact_encounter.",
        "billed_amount": "Amount the provider billed for the claim (USD).",
        "allowed_amount": "Contractually allowed amount after payer adjustment (USD).",
        "paid_amount": "Amount actually paid by the payer (USD).",
        "denial_flag": "1 if the claim was denied.",
        "denial_risk_score": "ML-predicted probability the claim is denied (0-1).",
        "denial_risk_category": "Banded denial risk: High, Medium, or Low.",
        "primary_denial_reason": "Primary reason the claim was (or is predicted to be) denied.",
    },
    "fact_prescription": {
        "prescription_key": "Surrogate primary key of the fill.",
        "fill_date_key": "Date the prescription was filled; join to dim_date.date_key.",
        "medication_key": "FK to dim_medication.",
        "days_supply": "Days of medication supplied by this fill (drives PDC).",
        "is_generic": "1 if a generic medication was dispensed.",
        "is_chronic_medication": "1 if the medication is for a chronic condition.",
        "total_cost": "Total cost of the fill (USD).",
        "payer_paid": "Portion of the fill cost paid by the payer (USD).",
        "patient_copay": "Patient out-of-pocket copay for the fill (USD).",
    },
    "fact_diagnosis": {
        "fact_diagnosis_key": "Surrogate primary key.",
        "encounter_key": "FK to fact_encounter.",
        "diagnosis_key": "FK to dim_diagnosis.",
        "diagnosis_sequence": "Order of the diagnosis on the encounter; 1 = primary diagnosis.",
        "present_on_admission": "Present-on-admission indicator (Y/N/U/W).",
    },
    "agg_readmission_by_date": {
        "encounter_date_key": "Date grain; join to dim_date.date_key.",
        "total_encounters": "Total encounters for the date and encounter_type.",
        "actual_readmissions": "Count of encounters that were readmissions.",
        "avg_risk_score": "Average predicted readmission risk for the group.",
        "high_risk_count": "Encounters classified High readmission risk.",
    },
    "agg_denial_by_date": {
        "claim_date_key": "Date grain; join to dim_date.date_key.",
        "total_claims": "Total claims for the date and claim_type.",
        "actual_denials": "Count of denied claims.",
        "at_risk_amount": "Billed dollars on claims flagged at denial risk (USD).",
        "avg_risk_score": "Average predicted denial risk for the group.",
    },
    "agg_medication_adherence": {
        "patient_key": "FK to dim_patient.",
        "medication_key": "FK to dim_medication.",
        "pdc_score": "Proportion of Days Covered over the measurement period (0-1).",
        "adherence_category": "Adherence band: Adherent (PDC >= 0.8), Partial, or Non-Adherent.",
        "gap_days": "Number of days in the period with no medication on hand.",
        "is_chronic": "1 if the adherence row is for a chronic medication.",
    },
}

_tbl_ok = 0
for _tbl, _desc in TABLE_COMMENTS.items():
    try:
        spark.sql(f"COMMENT ON TABLE {_tbl} IS '{_esc(_desc)}'")
        _tbl_ok += 1
    except Exception as _e:
        print(f"  [WARN] table comment {_tbl}: {_e}")

_col_ok = 0
for _tbl, _cols in COLUMN_COMMENTS.items():
    for _col, _cdesc in _cols.items():
        try:
            spark.sql(f"ALTER TABLE {_tbl} ALTER COLUMN {_col} COMMENT '{_esc(_cdesc)}'")
            _col_ok += 1
        except Exception as _e:
            print(f"  [WARN] column comment {_tbl}.{_col}: {_e}")

print(f"✓ Applied {_tbl_ok} table descriptions and {_col_ok} column descriptions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Summary

# CELL ********************

print("="*70)
print("🏆 GOLD LAKEHOUSE TABLES CREATED")
print("="*70)

tables = spark.sql("SHOW TABLES").collect()
print(f"\nTotal tables: {len(tables)}")

print("\n📊 DIMENSION TABLES:")
for t in tables:
    if t['tableName'].startswith('dim_'):
        print(f"   • {t['tableName']}")

print("\n📈 FACT TABLES:")
for t in tables:
    if t['tableName'].startswith('fact_'):
        print(f"   • {t['tableName']}")

print("\n📊 AGGREGATE TABLES:")
for t in tables:
    if t['tableName'].startswith('agg_'):
        print(f"   • {t['tableName']}")

print("\n" + "="*70)
print("NEXT STEPS:")
print("="*70)
print("1. Run the Warehouse stored procedures (02b_Stored_Procedures_Lakehouse_Target.sql)")
print("2. The procs will WRITE to these lh_gold_curated tables")
print("3. Connect Power BI using DIRECT LAKE to this lakehouse")
print("\n✅ Best of both worlds: T-SQL procs + Direct Lake performance!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
