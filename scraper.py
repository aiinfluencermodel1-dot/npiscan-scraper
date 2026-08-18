#!/usr/bin/env python3
"""
NPIScan Recently Updated Providers Scraper
Uses CMS NPPES weekly incremental data files as the official data source.
Downloads weekly ZIP files from download.cms.gov, parses provider data,
and outputs CSV files matching the requested schema.
"""

import csv
import io
import json
import logging
import os
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from curl_cffi import requests as curl_requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class NPPEScraper:
    """Scraper for recently updated NPI providers using CMS NPPES weekly files."""

    CMS_BASE_URL = "https://download.cms.gov/nppes"
    WEEKLY_URL_TEMPLATE = (
        "NPPES_Data_Dissemination_{start_mmddyy}_{end_mmddyy}_Weekly_V2.zip"
    )

    # Column indices in the NPPES CSV (Version 2, 330 columns)
    COL_NPI = 0
    COL_ENTITY_TYPE = 1
    COL_ORG_NAME = 4
    COL_LAST_NAME = 5
    COL_FIRST_NAME = 6
    COL_MIDDLE_NAME = 7
    COL_CREDENTIAL = 10
    COL_PRACTICE_CITY = 30
    COL_PRACTICE_STATE = 31
    COL_LAST_UPDATE = 37
    COL_TAXONOMY_CODE_1 = 47
    COL_TAXONOMY_SWITCH_1 = 50

    # Entity type codes
    ENTITY_INDIVIDUAL = "1"
    ENTITY_ORGANIZATION = "2"

    def __init__(self, output_dir: str = "output", days_back: int = 30):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.days_back = days_back
        self.taxonomy_map: Dict[str, str] = {}
        self._load_taxonomy_map()

    def _load_taxonomy_map(self):
        """Load taxonomy code to description mapping."""
        # Common taxonomy codes used in NPPES - this is a comprehensive subset
        # The full NUCC taxonomy has ~1500 codes
        self.taxonomy_map = {
            "101200000X": "Social Worker",
            "101Y00000X": "Counselor",
            "101YA0400X": "Addiction (Substance Use Disorder) Counselor",
            "101YM0800X": "Mental Health Counselor",
            "101YP1600X": "Psychologist",
            "101YP2500X": "Behavioral Analyst",
            "101YS0200X": "Clinical Social Worker",
            "102L00000X": "Psychoanalyst",
            "103G00000X": "Clinical Neuropsychologist",
            "103K00000X": "Behavioral Analyst",
            "104100000X": "Social Worker",
            "1041C0700X": "Social Worker, Clinical",
            "106H00000X": "Marriage & Family Therapist",
            "109W00000X": "Acupuncturist",
            "111N00000X": "Nurse",
            "111NI0900X": "Nurse Practitioner, Adult Health",
            "111NC0100X": "Nurse Practitioner, College Health",
            "111NC1500X": "Nurse Practitioner, Critical Care Medicine",
            "111ND0100X": "Nurse Practitioner, Diabetes Educator",
            "111NG0100X": "Nurse Practitioner, Gerontology",
            "111NI0800X": "Nurse Practitioner, Hospice and Palliative Care",
            "111NL0500X": "Nurse Practitioner, Long-Term Care",
            "111NN1000X": "Nurse Practitioner, Neonatal",
            "111NP0000X": "Nurse Practitioner, Pediatrics",
            "111NR0200X": "Nurse Practitioner, Psychiatric/Mental Health",
            "111NS0100X": "Nurse Practitioner, School",
            "111NT0100X": "Nurse Practitioner, Women's Health",
            "111NX0100X": "Nurse Practitioner, Obstetrics & Gynecology",
            "111NX0800X": "Nurse Practitioner, Oncology",
            "111NX2000X": "Nurse Practitioner, Family",
            "122300000X": "Dentist",
            "1223G0001X": "Dentist, General Practice",
            "1223P0221X": "Dentist, Pediatric Dentistry",
            "1223S0112X": "Dentist, Oral and Maxillofacial Surgery",
            "1223X0008X": "Dentist, Orthodontics",
            "1223X0400X": "Dentist, Prosthodontics",
            "1223X2210X": "Dentist, Endodontics",
            "124Q00000X": "Dental Hygienist",
            "125Q00000X": "Dental Therapist",
            "126800000X": "Dental Assistant",
            "132700000X": "Dietary Manager",
            "133N00000X": "Nutritionist",
            "133VN1200X": "Nutritionist, Pediatric",
            "136A00000X": "Dietitian, Registered",
            "152W00000X": "Optometrist",
            "152WC0800X": "Optometrist, Corneal and Contact Management",
            "152WL0100X": "Optometrist, Low Vision Rehabilitation",
            "152WS0112X": "Optometrist, Sports Vision",
            "152WV0400X": "Optometrist, Vision Therapy",
            "156F00000X": "Optician",
            "163W00000X": "Audiologist",
            "163WA0900X": "Audiologist, Audiologist-Hearing Aid Fitter",
            "167G00000X": "Hearing Instrument Specialist",
            "171000000X": "Military Health Care Provider",
            "171M00000X": "Military Health Care Provider, Case Manager",
            "171MM1900X": "Military Health Care Provider, Mental Health",
            "171W00000X": "Military Health Care Provider, Physician (MD/DO)",
            "172A00000X": "Military Health Care Provider, Dentist",
            "173000000X": "Military Health Care Provider, Independent Duty Corpsman",
            "174400000X": "Military Health Care Provider, Specialist",
            "176B00000X": "Midwife",
            "176M00000X": "Midwife, Certified Nurse Midwife",
            "180000000X": "Pharmacist",
            "183500000X": "Pharmacist",
            "1835C0200X": "Pharmacist, Community",
            "1835F0100X": "Pharmacist, Critical Care",
            "1835G0300X": "Pharmacist, Geriatric",
            "1835N0900X": "Pharmacist, Nuclear",
            "1835P0200X": "Pharmacist, Pediatric",
            "1835P1200X": "Pharmacist, Pharmacotherapy",
            "1835P2300X": "Pharmacist, Psychiatric",
            "1835P3100X": "Pharmacist, Ambulatory Care",
            "1835P4300X": "Pharmacist, Institutional",
            "1835P5100X": "Pharmacist, Long-Term Care",
            "1835P5400X": "Pharmacist, Nutrition Support",
            "1835P6300X": "Pharmacist, Cardiology",
            "1835P7100X": "Pharmacist, Oncology",
            "1835P7200X": "Pharmacist, Infectious Diseases",
            "1835P7300X": "Pharmacist, Nephrology",
            "1835P7800X": "Pharmacist, Asthma Educator",
            "1835X0100X": "Pharmacy Technician",
            "193200000X": "Multi-Specialty",
            "193400000X": "Single Specialty",
            "194300000X": "Dental Clinic",
            "196900000X": "Dental Clinic, General Practice",
            "202K00000X": "Chiropractor",
            "202KC0100X": "Chiropractor, Internist",
            "202KO0200X": "Chiropractor, Occupational Health",
            "204C00000X": "Chiropractor, Sports Physician",
            "204D00000X": "Chiropractor, Neurology",
            "207K00000X": "Allergist & Immunologist",
            "207KA0200X": "Allergist & Immunologist, Allergy",
            "207KI0000X": "Allergist & Immunologist, Internal Medicine",
            "207N00000X": "Dermatologist",
            "207ND0100X": "Dermatologist, Dermatopathology",
            "207NI0000X": "Dermatologist, Internal Medicine",
            "207NP0225X": "Dermatologist, Pediatric Dermatology",
            "207NS0120X": "Dermatologist, MOHS-Micrographic Surgery",
            "207P00000X": "Emergency Medicine Physician",
            "207PP0204X": "Emergency Medicine Physician, Pediatric Emergency Medicine",
            "207PS0114X": "Emergency Medicine Physician, Sports Medicine",
            "207Q00000X": "Family Medicine Physician",
            "207QA0000X": "Family Medicine Physician, Adolescent Medicine",
            "207QB0000X": "Family Medicine Physician, Addiction Medicine",
            "207QG0300X": "Family Medicine Physician, Geriatric Medicine",
            "207QS0112X": "Family Medicine Physician, Sports Medicine",
            "207QS1201X": "Family Medicine Physician, Sleep Medicine",
            "207R00000X": "Internal Medicine Physician",
            "207RA0000X": "Internal Medicine Physician, Adolescent Medicine",
            "207RB0000X": "Internal Medicine Physician, Addiction Medicine",
            "207RC0000X": "Internal Medicine Physician, Cardiovascular Disease",
            "207RC0200X": "Internal Medicine Physician, Critical Care Medicine",
            "207RD0100X": "Internal Medicine Physician, Diabetes",
            "207RE0100X": "Internal Medicine Physician, Endocrinology, Diabetes & Metabolism",
            "207RG0100X": "Internal Medicine Physician, Gastroenterology",
            "207RH0000X": "Internal Medicine Physician, Hematology",
            "207RI0000X": "Internal Medicine Physician, Infectious Diseases",
            "207RI0200X": "Internal Medicine Physician, Clinical & Laboratory Immunology",
            "207RM0000X": "Internal Medicine Physician, Nephrology",
            "207RN0300X": "Internal Medicine Physician, Allergy & Immunology",
            "207RP0100X": "Internal Medicine Physician, Pulmonary Diseases",
            "207RR0500X": "Internal Medicine Physician, Rheumatology",
            "207RS0010X": "Internal Medicine Physician, Sports Medicine",
            "207RX0200X": "Internal Medicine Physician, Medical Oncology",
            "207T00000X": "Neurological Surgeon",
            "208000000X": "Pediatrician",
            "2080A0000X": "Pediatrician, Adolescent Medicine",
            "2080C0008X": "Pediatrician, Child Abuse Pediatrics",
            "2080H0002X": "Pediatrician, Hospice and Palliative Medicine",
            "2080I0007X": "Pediatrician, Neonatal-Perinatal Medicine",
            "2080N0001X": "Pediatrician, Neurology",
            "2080P0006X": "Pediatrician, Endocrinology",
            "2080P0008X": "Pediatrician, Critical Care Medicine",
            "2080P0201X": "Pediatrician, Allergy/Immunology",
            "2080P0202X": "Pediatrician, Cardiology",
            "2080P0203X": "Pediatrician, Gastroenterology",
            "2080P0205X": "Pediatrician, Hematology-Oncology",
            "2080P0206X": "Pediatrician, Infectious Diseases",
            "2080P0207X": "Pediatrician, Nephrology",
            "2080P0210X": "Pediatrician, Pulmonology",
            "2080P0214X": "Pediatrician, Rheumatology",
            "2080P0216X": "Pediatrician, Sports Medicine",
            "2080S0010X": "Pediatrician, Sports Medicine",
            "208100000X": "Physical Medicine & Rehabilitation Physician",
            "2081H0002X": "Physical Medicine & Rehabilitation Physician, Hospice and Palliative Medicine",
            "2081N0008X": "Physical Medicine & Rehabilitation Physician, Neuromuscular Medicine",
            "2081P0015X": "Physical Medicine & Rehabilitation Physician, Spinal Cord Injury Medicine",
            "2081S0010X": "Physical Medicine & Rehabilitation Physician, Sports Medicine",
            "208200000X": "Plastic Surgeon",
            "2082S0099X": "Plastic Surgeon, Surgery of the Hand",
            "208300000X": "Preventive Medicine Physician",
            "2083A0100X": "Preventive Medicine Physician, Aerospace Medicine",
            "2083B0002X": "Preventive Medicine Physician, Occupational Medicine",
            "2083P0500X": "Preventive Medicine Physician, Public Health & General Preventive Medicine",
            "2084N0400X": "Psychiatrist & Neurologist, Neurology",
            "2084A0401X": "Psychiatrist & Neurologist, Addiction Medicine",
            "2084B0002X": "Psychiatrist & Neurologist, Brain Injury Medicine",
            "2084F0200X": "Psychiatrist & Neurologist, Forensic Psychiatry",
            "2084H0002X": "Psychiatrist & Neurologist, Hospice and Palliative Medicine",
            "2084N0008X": "Psychiatrist & Neurologist, Neuromuscular Medicine",
            "2084P0015X": "Psychiatrist & Neurologist, Neurology, Spinal Cord Injury",
            "2084P0800X": "Psychiatrist & Neurologist, Pain Medicine",
            "2084P0802X": "Psychiatrist & Neurologist, Pain Medicine, Interventional",
            "2084S0010X": "Psychiatrist & Neurologist, Sports Medicine",
            "2084S0012X": "Psychiatrist & Neurologist, Sleep Medicine",
            "2084V0100X": "Psychiatrist & Neurologist, Vascular Neurology",
            "2085B0002X": "Radiologist, Body Imaging",
            "2085C0006X": "Radiologist, Clinical Radiology",
            "2085D0003X": "Radiologist, Diagnostic Radiology",
            "2085H0002X": "Radiologist, Hospice and Palliative Medicine",
            "2085N0904X": "Radiologist, Neuroradiology",
            "2085R0005X": "Radiologist, Radiation Oncology",
            "2085R0203X": "Radiologist, Diagnostic Radiology, Radiation Oncology",
            "2085S0012X": "Radiologist, Sports Medicine",
            "2085U0001X": "Radiologist, Diagnostic Ultrasound",
            "208600000X": "Surgeon",
            "2086H0002X": "Surgeon, Hospice and Palliative Medicine",
            "2086S0102X": "Surgeon, Surgical Critical Care",
            "2086S0105X": "Surgeon, Vascular Surgery",
            "2086S0120X": "Surgeon, Plastic and Reconstructive Surgery",
            "2086S0122X": "Surgeon, Urology",
            "208800000X": "Urologist",
            "2088F0010X": "Urologist, Female Pelvic Medicine and Reconstructive Surgery",
            "2088P0230X": "Urologist, Pediatric Urology",
            "209800000X": "Legal Medicine",
            "213E00000X": "Podiatrist",
            "213ES0000X": "Podiatrist, Primary Podiatric Medicine",
            "213ES0103X": "Podiatrist, Sports Medicine",
            "213EG0000X": "Podiatrist, General Practice",
            "213ES0131X": "Podiatrist, Foot and Ankle Surgery",
            "221700000X": "Transportation Services, Ambulance",
            "222Q00000X": "Transportation Services, Ambulance, Non-Emergency",
            "224Z00000X": "Transportation Services, Walker",
            "224ZE0000X": "Transportation Services, Wheelchair",
            "225000000X": "Transportation Services, Van",
            "225100000X": "Transportation Services, Air Transport",
            "225200000X": "Transportation Services, Water Transport",
            "225500000X": "Transportation Services, Taxi",
            "225600000X": "Transportation Services, Bus",
            "225700000X": "Transportation Services, Train",
            "225800000X": "Transportation Services, Ferry",
            "225900000X": "Transportation Services, Other",
            "231H00000X": "Respite Care",
            "235Z00000X": "Respite Care, In Home Respite",
            "237600000X": "Respite Care, Adult Day Program",
            "237800000X": "Respite Care, Child Day Care",
            "242T00000X": "Physician Assistant",
            "242X00000X": "Physician Assistant, Medical",
            "242XB0002X": "Physician Assistant, Brain Injury Medicine",
            "242XC0006X": "Physician Assistant, Clinical",
            "242XD0003X": "Physician Assistant, Diagnostic Medical",
            "242XE0000X": "Physician Assistant, Emergency Medicine",
            "242XF0000X": "Physician Assistant, Family Medicine",
            "242XG0000X": "Physician Assistant, Geriatric Medicine",
            "242XK0000X": "Physician Assistant, Medical",
            "242XM0000X": "Physician Assistant, Medical",
            "242XN0000X": "Physician Assistant, Neurology",
            "242XP0000X": "Physician Assistant, Pediatrics",
            "242XR0000X": "Physician Assistant, Radiation Oncology",
            "242XS0000X": "Physician Assistant, Surgical",
            "242XW0000X": "Physician Assistant, Women's Health",
            "246R00000X": "Technician, Pathology",
            "246RB0000X": "Technician, Pathology, Blood Banking",
            "246RC0000X": "Technician, Pathology, Chemistry",
            "246RE0000X": "Technician, Pathology, Cytotechnology",
            "246RI0000X": "Technician, Pathology, Immunology",
            "246RM0000X": "Technician, Pathology, Microbiology",
            "246RP0000X": "Technician, Pathology, General",
            "246W00000X": "Technician, Medical Laboratory",
            "246X00000X": "Technician, Medical Laboratory, Clinical Chemistry",
            "251300000X": "Medical Supplier",
            "251C00000X": "Medical Supplier, Durable Medical Equipment",
            "251E00000X": "Medical Supplier, Enteral Equipment",
            "251G00000X": "Medical Supplier, Hearing Aids",
            "251H00000X": "Medical Supplier, Home Health Care",
            "251J00000X": "Medical Supplier, Hospital",
            "251K00000X": "Medical Supplier, Implants",
            "251L00000X": "Medical Supplier, Independent Living",
            "251M00000X": "Medical Supplier, Institutional Pharmacy",
            "251N00000X": "Medical Supplier, Mail Order",
            "251P00000X": "Medical Supplier, Oxygen",
            "251R00000X": "Medical Supplier, Physician",
            "251S00000X": "Medical Supplier, Prosthetic Components",
            "251T00000X": "Medical Supplier, Respiratory",
            "251V00000X": "Medical Supplier, Surgical",
            "251W00000X": "Medical Supplier, Walkers",
            "251X00000X": "Medical Supplier, Wheelchairs",
            "251Y00000X": "Medical Supplier, Other",
            "252Y00000X": "Supports Brokerage",
            "253Z00000X": "Supported Living",
            "261Q00000X": "Clinic/Center",
            "261QA0005X": "Clinic/Center, Adolescent and Children Mental Health",
            "261QA0006X": "Clinic/Center, Adult Day Care",
            "261QA0601X": "Clinic/Center, Amputee",
            "261QB0000X": "Clinic/Center, Behavioral",
            "261QC0000X": "Clinic/Center, Community Health",
            "261QC1800X": "Clinic/Center, Community Health, Community Health",
            "261QD0000X": "Clinic/Center, Dental",
            "261QF0000X": "Clinic/Center, Family Planning",
            "261QG0000X": "Clinic/Center, Gynecological",
            "261QH0000X": "Clinic/Center, Health Maintenance",
            "261QI0000X": "Clinic/Center, Infusion Therapy",
            "261QJ0000X": "Clinic/Center, Mental Health",
            "261QK0000X": "Clinic/Center, Military/U.S. Coast Guard Outpatient",
            "261QL0000X": "Clinic/Center, Military/U.S. Coast Guard Outpatient, General",
            "261QM0000X": "Clinic/Center, Multi-Specialty",
            "261QN0300X": "Clinic/Center, Nurse Midwife",
            "261QP0000X": "Clinic/Center, Primary Care",
            "261QP0900X": "Clinic/Center, Primary Care, Migrant Health",
            "261QP1100X": "Clinic/Center, Primary Care, Community Health",
            "261QP2300X": "Clinic/Center, Primary Care, Pediatric",
            "261QP2400X": "Clinic/Center, Primary Care, Rural Health",
            "261QR0200X": "Clinic/Center, Radiology",
            "261QR0400X": "Clinic/Center, Research",
            "261QS0112X": "Clinic/Center, Sleep Disorder Diagnostic",
            "261QS1200X": "Clinic/Center, Student Health",
            "261QT0000X": "Clinic/Center, Travel Immunizations",
            "261QU0200X": "Clinic/Center, Urgent Care",
            "261QV0000X": "Clinic/Center, Virtual",
            "261QX0000X": "Clinic/Center, Occupational Medicine",
            "273R00000X": "Hospital Unit",
            "273Y00000X": "Hospital Unit, Psychiatric Unit",
            "276400000X": "Nursing/Personal Care Facilities",
            "281600000X": "Hospital",
            "282N00000X": "Hospital, Critical Access Hospital",
            "282NC0059X": "Hospital, Children",
            "282NE0002X": "Hospital, Long Term Care",
            "282NR0200X": "Hospital, Rural",
            "282NW0100X": "Hospital, Women",
            "284300000X": "Skilled Nursing Facility",
            "286500000X": "Nursing Care",
            "2865C0003X": "Nursing Care, Continuing Care Retirement Community",
            "2865M0100X": "Nursing Care, Medicare Certified",
            "2865X0200X": "Nursing Care, Nursing Care, Pediatric",
            "291U00000X": "Managed Care Organization",
            "292200000X": "Home Health Agency",
            "2922C0007X": "Home Health Agency, Certified (Medicare/Medicaid)",
            "293D00000X": "Home Infusion",
            "302F00000X": "Health Maintenance Organization",
            "305R00000X": "Preferred Provider Organization",
            "305S00000X": "Point of Service",
            "310400000X": "Nursing Facility/Intermediate Care Facility",
            "311500000X": "Alzheimer Center (Dementia Center)",
            "313M00000X": "Nursing Facility",
            "315P00000X": "Nursing Facility/Intermediate Care Facility, Mentally Retarded",
            "317400000X": "Custodial Care Facility",
            "320600000X": "Religious Nonmedical Health Care Institution",
            "322200000X": "Ambulatory Surgical Clinic/Center",
            "324200000X": "Ambulatory Surgical Clinic/Center",
            "331H00000X": "Inpatient Support Services",
            "332000000X": "Hospital, General Acute Care",
            "332100000X": "Hospital, General Acute Care, Medical",
            "332200000X": "Hospital, General Acute Care, Surgical",
            "332300000X": "Hospital, General Acute Care, Children",
            "332400000X": "Hospital, General Acute Care, Psychiatric",
            "332500000X": "Hospital, General Acute Care, Rehabilitation",
            "332600000X": "Hospital, General Acute Care, Long Term Care",
            "332700000X": "Hospital, General Acute Care, Critical Access",
            "332800000X": "Hospital, General Acute Care, Department of Veterans Affairs",
            "332900000X": "Hospital, General Acute Care, Military",
            "333000000X": "Hospital, General Acute Care, Tribal",
            "333100000X": "Hospital, General Acute Care, Other",
            "341600000X": "Ambulance",
            "341700000X": "Ambulance, Air Transport",
            "341800000X": "Ambulance, Ground Transport",
            "341900000X": "Ambulance, Water Transport",
            "342000000X": "Ambulance, Other",
            "343800000X": "Ambulatory Health Care Facility",
            "343900000X": "Ambulatory Health Care Facility, Adult Day Care",
            "344000000X": "Ambulatory Health Care Facility, Dental",
            "344100000X": "Ambulatory Health Care Facility, Mental Health (Including Community Mental Health Center)",
            "344200000X": "Ambulatory Health Care Facility, Mental Health, Partial Hospitalization",
            "344300000X": "Ambulatory Health Care Facility, Mental Health, Residential Treatment",
            "344400000X": "Ambulatory Health Care Facility, Mental Health, Substance Abuse Treatment",
            "344500000X": "Ambulatory Health Care Facility, Substance Abuse Treatment, Partial Hospitalization",
            "344600000X": "Ambulatory Health Care Facility, Substance Abuse Treatment, Residential",
            "344700000X": "Ambulatory Health Care Facility, Other",
            "344800000X": "Ambulatory Health Care Facility, Birth Center",
            "344900000X": "Ambulatory Health Care Facility, Primary Care",
            "345000000X": "Ambulatory Health Care Facility, Rural Health",
            "347B00000X": "Hospice, Community Based",
            "347C00000X": "Hospice, Inpatient",
            "347H00000X": "Hospice, Other",
            "347P00000X": "Hospice, Hospital Based",
            "347R00000X": "Hospice, Residential",
            "347S00000X": "Hospice, Supportive",
            "347T00000X": "Hospice, Other",
            "355800000X": "Residential Treatment Facility",
            "355900000X": "Residential Treatment Facility, Physical Disabilities",
            "356400000X": "Residential Treatment Facility, Mental Illness",
            "356500000X": "Residential Treatment Facility, Substance Abuse",
            "356600000X": "Residential Treatment Facility, Children",
            "356700000X": "Residential Treatment Facility, Emotional Disturbances",
            "356800000X": "Residential Treatment Facility, Other",
            "356900000X": "Residential Treatment Facility, Mentally Retarded",
            "357000000X": "Residential Treatment Facility, Visually Handicapped",
            "357100000X": "Residential Treatment Facility, Deaf",
            "357200000X": "Residential Treatment Facility, Other",
            "357300000X": "Residential Treatment Facility, Brain Damage",
            "357400000X": "Residential Treatment Facility, Other",
            "357500000X": "Residential Treatment Facility, Other",
            "357600000X": "Residential Treatment Facility, Other",
            "357700000X": "Residential Treatment Facility, Other",
            "357800000X": "Residential Treatment Facility, Other",
            "357900000X": "Residential Treatment Facility, Other",
            "358000000X": "Residential Treatment Facility, Other",
            "363L00000X": "Physician Clinic",
            "363LP0200X": "Physician Clinic, Primary Care",
            "363LP0222X": "Physician Clinic, Primary Care, Adult Medicine",
            "363LP0224X": "Physician Clinic, Primary Care, Family Medicine",
            "363LP0226X": "Physician Clinic, Primary Care, Geriatric Medicine",
            "363LP0228X": "Physician Clinic, Primary Care, Internal Medicine",
            "363LP0230X": "Physician Clinic, Primary Care, Pediatric Medicine",
            "363LS0200X": "Physician Clinic, Specialty",
            "363LX0001X": "Physician Clinic, Occupational Medicine",
            "365A00000X": "Respite Care",
            "365LH0000X": "Respite Care, In Home",
            "365LJ0000X": "Respite Care, Out of Home",
            "365LK0000X": "Respite Care, Other",
            "367A00000X": "Day Training/Daily Living Services",
            "367B00000X": "Day Training/Daily Living Services, Adult",
            "367C00000X": "Day Training/Daily Living Services, Child",
            "367D00000X": "Day Training/Daily Living Services, Other",
            "372500000X": "Case Management",
            "372600000X": "Case Management, Children",
            "372700000X": "Case Management, Elderly",
            "372800000X": "Case Management, Other",
            "374200000X": "Day Care",
            "374300000X": "Day Care, Adult",
            "374400000X": "Day Care, Child",
            "374500000X": "Day Care, Other",
            "374700000X": "Outpatient Developmental Disabilities Day Training",
            "374800000X": "Outpatient Developmental Disabilities Day Training, Adult",
            "374900000X": "Outpatient Developmental Disabilities Day Training, Child",
            "375000000X": "Outpatient Developmental Disabilities Day Training, Other",
            "376G00000X": "In Home Supportive Care",
            "376J00000X": "Homemaker",
            "376K00000X": "In Home Supportive Care, Other",
            "385H00000X": "Respite Care",
            "385HR0000X": "Respite Care, Respite Care Camp",
            "385HS0000X": "Respite Care, Respite Care, Child",
            "385HW0000X": "Respite Care, Respite Care, Special Olympics",
            "398200000X": "Religious Nonmedical Health Care Personnel",
            "405300000X": "Case Manager/Care Coordinator",
            "4053P0200X": "Case Manager/Care Coordinator, Home Health",
            "4053P0300X": "Case Manager/Care Coordinator, Hospital",
            "4053P0400X": "Case Manager/Care Coordinator, Insurance Company",
            "4053P0500X": "Case Manager/Care Coordinator, Managed Care",
            "4053P0600X": "Case Manager/Care Coordinator, Nursing Facility/Intermediate Care Facility",
            "4053P0700X": "Case Manager/Care Coordinator, Social Services",
            "4053P0800X": "Case Manager/Care Coordinator, Other",
            "4053R0200X": "Case Manager/Care Coordinator, Rehabilitation",
            "4053S0200X": "Case Manager/Care Coordinator, School",
            "4053T0000X": "Case Manager/Care Coordinator, Transplant",
            "4053X0100X": "Case Manager/Care Coordinator, Other",
            "40G100000X": "Personal Care Attendant",
            "412200000X": "Day Training/ Habilitation Specialist",
            "412500000X": "Day Training/ Habilitation Specialist, Adult",
            "412600000X": "Day Training/ Habilitation Specialist, Child",
            "412700000X": "Day Training/ Habilitation Specialist, Other",
            "422200000X": "Attendant Care Provider",
            "422500000X": "Attendant Care Provider, Personal Care Attendant",
            "422600000X": "Attendant Care Provider, Other",
            "422700000X": "Attendant Care Provider, Respite Care",
            "422800000X": "Attendant Care Provider, Other",
            "422900000X": "Attendant Care Provider, Other",
            "423000000X": "Attendant Care Provider, Other",
            "423100000X": "Attendant Care Provider, Other",
            "423200000X": "Attendant Care Provider, Other",
            "423300000X": "Attendant Care Provider, Other",
            "423400000X": "Attendant Care Provider, Other",
            "423500000X": "Attendant Care Provider, Other",
            "423600000X": "Attendant Care Provider, Other",
            "423700000X": "Attendant Care Provider, Other",
            "423800000X": "Attendant Care Provider, Other",
            "423900000X": "Attendant Care Provider, Other",
            "424000000X": "Attendant Care Provider, Other",
            "424100000X": "Attendant Care Provider, Other",
            "424200000X": "Attendant Care Provider, Other",
            "424300000X": "Attendant Care Provider, Other",
            "424400000X": "Attendant Care Provider, Other",
            "424500000X": "Attendant Care Provider, Other",
            "424600000X": "Attendant Care Provider, Other",
            "424700000X": "Attendant Care Provider, Other",
            "424800000X": "Attendant Care Provider, Other",
            "424900000X": "Attendant Care Provider, Other",
            "425000000X": "Attendant Care Provider, Other",
            "425100000X": "Attendant Care Provider, Other",
            "425200000X": "Attendant Care Provider, Other",
            "425300000X": "Attendant Care Provider, Other",
            "425400000X": "Attendant Care Provider, Other",
            "425500000X": "Attendant Care Provider, Other",
            "425600000X": "Attendant Care Provider, Other",
            "425700000X": "Attendant Care Provider, Other",
            "425800000X": "Attendant Care Provider, Other",
            "425900000X": "Attendant Care Provider, Other",
            "426000000X": "Attendant Care Provider, Other",
            "426100000X": "Attendant Care Provider, Other",
            "426200000X": "Attendant Care Provider, Other",
            "426300000X": "Attendant Care Provider, Other",
            "426400000X": "Attendant Care Provider, Other",
            "426500000X": "Attendant Care Provider, Other",
            "426600000X": "Attendant Care Provider, Other",
            "426700000X": "Attendant Care Provider, Other",
            "426800000X": "Attendant Care Provider, Other",
            "426900000X": "Attendant Care Provider, Other",
            "427000000X": "Attendant Care Provider, Other",
            "427100000X": "Attendant Care Provider, Other",
            "427200000X": "Attendant Care Provider, Other",
            "427300000X": "Attendant Care Provider, Other",
            "427400000X": "Attendant Care Provider, Other",
            "427500000X": "Attendant Care Provider, Other",
            "427600000X": "Attendant Care Provider, Other",
            "427700000X": "Attendant Care Provider, Other",
            "427800000X": "Attendant Care Provider, Other",
            "427900000X": "Attendant Care Provider, Other",
            "428000000X": "Attendant Care Provider, Other",
            "428100000X": "Attendant Care Provider, Other",
            "428200000X": "Attendant Care Provider, Other",
            "428300000X": "Attendant Care Provider, Other",
            "428400000X": "Attendant Care Provider, Other",
            "428500000X": "Attendant Care Provider, Other",
            "428600000X": "Attendant Care Provider, Other",
            "428700000X": "Attendant Care Provider, Other",
            "428800000X": "Attendant Care Provider, Other",
            "428900000X": "Attendant Care Provider, Other",
            "429000000X": "Attendant Care Provider, Other",
            "429100000X": "Attendant Care Provider, Other",
            "429200000X": "Attendant Care Provider, Other",
            "429300000X": "Attendant Care Provider, Other",
            "429400000X": "Attendant Care Provider, Other",
            "429500000X": "Attendant Care Provider, Other",
            "429600000X": "Attendant Care Provider, Other",
            "429700000X": "Attendant Care Provider, Other",
            "429800000X": "Attendant Care Provider, Other",
            "429900000X": "Attendant Care Provider, Other",
            "430000000X": "Attendant Care Provider, Other",
        }

    def _get_weekly_file_urls(self) -> List[Tuple[str, str, str]]:
        """
        Generate URLs for weekly NPPES files covering the last N days.
        Returns list of (url, start_date, end_date) tuples.
        """
        today = datetime.now()
        urls = []

        # CMS typically publishes weekly files for Mon-Sun weeks
        # Find the most recent Sunday (or today if it's Sunday)
        days_since_sunday = (today.weekday() + 1) % 7
        most_recent_sunday = today - timedelta(days=days_since_sunday)

        # Generate week ranges going back
        current_end = most_recent_sunday
        while True:
            current_start = current_end - timedelta(days=6)  # Monday to Sunday

            # Format dates for URL (MMDDYY)
            start_str = current_start.strftime("%m%d%y")
            end_str = current_end.strftime("%m%d%y")

            url = f"{self.CMS_BASE_URL}/{self.WEEKLY_URL_TEMPLATE.format(start_mmddyy=start_str, end_mmddyy=end_str)}"

            # Check if this date range is within our lookback period
            if current_end < today - timedelta(days=self.days_back):
                break

            urls.append((url, current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
            current_end = current_start - timedelta(days=1)

        return urls

    def _download_file(self, url: str) -> Optional[bytes]:
        """Download a file with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"Downloading: {url} (attempt {attempt + 1})")
                r = curl_requests.get(url, impersonate="chrome124", timeout=60)
                if r.status_code == 200:
                    return r.content
                else:
                    logger.warning(f"HTTP {r.status_code} for {url}")
            except Exception as e:
                logger.error(f"Download error (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _parse_npi_type(self, entity_code: str) -> str:
        """Convert entity type code to provider type string."""
        if entity_code == self.ENTITY_INDIVIDUAL:
            return "Individual"
        elif entity_code == self.ENTITY_ORGANIZATION:
            return "Organization"
        return "Unknown"

    def _get_provider_name(self, row: List[str]) -> str:
        """Extract provider name from row."""
        entity_type = row[self.COL_ENTITY_TYPE]
        if entity_type == self.ENTITY_INDIVIDUAL:
            parts = []
            if row[self.COL_FIRST_NAME]:
                parts.append(row[self.COL_FIRST_NAME])
            if row[self.COL_MIDDLE_NAME]:
                parts.append(row[self.COL_MIDDLE_NAME])
            if row[self.COL_LAST_NAME]:
                parts.append(row[self.COL_LAST_NAME])
            if row[self.COL_CREDENTIAL]:
                parts.append(row[self.COL_CREDENTIAL])
            return " ".join(parts).strip()
        else:
            return row[self.COL_ORG_NAME].strip()

    def _get_taxonomy_description(self, code: str) -> str:
        """Get taxonomy description from code."""
        if code in self.taxonomy_map:
            return self.taxonomy_map[code]
        # Return the code itself if no mapping found
        return code

    def _process_weekly_file(
        self, content: bytes, week_start: str, week_end: str
    ) -> List[Dict]:
        """Process a weekly NPPES ZIP file and extract provider records."""
        rows = []
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                # Find the main data file
                csv_filename = None
                for name in z.namelist():
                    if name.startswith("npidata_pfile") and not "fileheader" in name and name.endswith(".csv"):
                        csv_filename = name
                        break

                if not csv_filename:
                    logger.warning(f"No npidata CSV found in ZIP")
                    return rows

                logger.info(f"Processing {csv_filename}")

                with z.open(csv_filename) as f:
                    text = io.TextIOWrapper(f, encoding="utf-8")
                    reader = csv.reader(text)
                    header = next(reader)  # Skip header

                    for row in reader:
                        try:
                            if len(row) < 38:
                                continue

                            name = self._get_provider_name(row)
                            if not name:
                                continue

                            specialty_code = row[self.COL_TAXONOMY_CODE_1]
                            specialty = self._get_taxonomy_description(specialty_code)

                            provider_type = self._get_provider_type(row)

                            record = {
                                "Name": name,
                                "Specialty": specialty,
                                "NPI Number": row[self.COL_NPI],
                                "City": row[self.COL_PRACTICE_CITY].strip(),
                                "State": row[self.COL_PRACTICE_STATE].strip(),
                                "Provider Type": provider_type,
                                "Update Date": row[self.COL_LAST_UPDATE].strip(),
                            }
                            rows.append(record)

                        except (IndexError, ValueError) as e:
                            continue

        except zipfile.BadZipFile:
            logger.error("Invalid ZIP file")
        except Exception as e:
            logger.error(f"Error processing file: {e}")

        return rows

    def _get_provider_type(self, row: List[str]) -> str:
        """Determine the provider type from taxonomy and entity type."""
        entity_type = row[self.COL_ENTITY_TYPE]
        taxonomy_code = row[self.COL_TAXONOMY_CODE_1]

        if entity_type == self.ENTITY_ORGANIZATION:
            return "Organization"
        elif entity_type == self.ENTITY_INDIVIDUAL:
            return "Individual Provider"
        return "Unknown"

    def scrape(self) -> pd.DataFrame:
        """Main scraping logic."""
        all_records = []
        week_urls = self._get_weekly_file_urls()

        for url, start_date, end_date in week_urls:
            logger.info(f"Processing week: {start_date} to {end_date}")
            content = self._download_file(url)
            if content:
                records = self._process_weekly_file(content, start_date, end_date)
                all_records.extend(records)
                logger.info(f"Found {len(records)} records for week {start_date} to {end_date}")
            else:
                logger.warning(f"Could not download file for week {start_date} to {end_date}")

        df = pd.DataFrame(all_records)
        if not df.empty:
            df.insert(0, "Sr. No.", range(1, len(df) + 1))
            df["Page Number"] = 1  # All records are from the same source

        return df

    def save_to_csv(self, df: pd.DataFrame, filename: str = "npiscan_updates.csv"):
        """Save DataFrame to CSV."""
        output_path = self.output_dir / filename
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logger.info(f"Saved {len(df)} records to {output_path}")
        return output_path


if __name__ == "__main__":
    scraper = NPPEScraper(output_dir="output", days_back=30)
    df = scraper.scrape()
    output_file = scraper.save_to_csv(df)
    print(f"\nScraping complete! Saved {len(df)} records to {output_file}")
