def detect_facility_type(name) -> str:
    if not name or not isinstance(name, str):
        return "Unknown"

    name = name.lower()

    if "hospital" in name:
        return "Hospital"
    if "clinic" in name:
        return "Clinic"
    if "pharmacy" in name:
        return "Pharmacy"
    if "dental" in name:
        return "Dental"
    if "eye" in name or "optic" in name:
        return "Eye Clinic"

    return "Unknown"