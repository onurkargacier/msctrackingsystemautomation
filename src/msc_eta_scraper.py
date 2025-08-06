import base64
import requests

def get_eta_etd(bl):
    eta = "Bilinmiyor"
    kaynak = "Bilinmiyor"
    export_date = "Bilinmiyor"

    try:
        param = f"trackingNumber={bl}&trackingMode=0"
        b64 = base64.b64encode(param.encode()).decode()
        url = f"https://www.msc.com/en/track-a-shipment?params={b64}"

        session = requests.Session()
        page = session.get(url)
        token = None

        for line in page.text.splitlines():
            if '__RequestVerificationToken' in line:
                token = line.split('value="')[1].split('"')[0]
                break

        if not token:
            raise Exception("Token alınamadı")

        api_url = "https://www.msc.com/api/feature/tools/TrackingInfo"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": "https://www.msc.com",
            "Referer": url,
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
            "__RequestVerificationToken": token,
        }
        payload = {
            "trackingNumber": bl,
            "trackingMode": "0"
        }

        resp = session.post(api_url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()

        b = data.get("Data", {}).get("BillOfLadings", [])
        if not b:
            return eta, kaynak, export_date

        bl_data = b[0]
        genel = bl_data.get("GeneralTrackingInfo", {})

        # ETA önceliklendirme
        for container in bl_data.get("ContainersInfo", []):
            for event in container.get("Events", []):
                desc = event.get("Description", "").strip().lower()
                if desc == "pod eta":
                    eta = event.get("Date")
                    kaynak = "POD ETA"
                    break
            if eta != "Bilinmiyor":
                break

        if eta == "Bilinmiyor" and genel.get("FinalPodEtaDate"):
            eta = genel["FinalPodEtaDate"]
            kaynak = "Final POD ETA"

        if eta == "Bilinmiyor":
            for container in bl_data.get("ContainersInfo", []):
                d = container.get("PodEtaDate")
                if d:
                    eta = d
                    kaynak = "Container POD ETA"
                    break

        if eta == "Bilinmiyor":
            for container in bl_data.get("ContainersInfo", []):
                for event in container.get("Events", []):
                    if event.get("Description", "").strip().lower() == "import to consignee":
                        eta = event.get("Date")
                        kaynak = "Import to consignee"
                        break

        # ETD → son konteynerin export loaded on vessel'ı
        containers = bl_data.get("ContainersInfo", [])
        if containers:
            last = containers[-1]
            for event in last.get("Events", []):
                if event.get("Description", "").strip().lower() == "export loaded on vessel":
                    export_date = event.get("Date")
                    break

    except Exception as e:
        print(f"[{bl}] Hata: {e}")

    return eta, kaynak, export_date
