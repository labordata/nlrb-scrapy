import csv
import datetime

import scrapy


class NLRBSpider(scrapy.Spider):
    name = "cases"

    def __init__(self, cases_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cases_file = cases_file

    def start_requests(self):
        with open(self.cases_file) as f:
            reader = csv.reader(f)
            for (case_number,) in reader:
                yield scrapy.Request(
                    url=f"https://www.nlrb.gov/case/{case_number}",
                    callback=self.parse_case,
                )

    def parse_case(self, response):

        # Case Name
        details = {}

        try:
            name = response.xpath(
                "//h1[@class='uswds-page-title page-title']/text()"
            ).get()
        except ValueError:
            response.status_code = 404

        details["name"] = name.strip()

        # Basic Details
        basic_info, *tally_elements = response.xpath(
            "//div[@id='block-mainpagecontent']/div[@class='display-flex flex-justify flex-wrap']"
        )

        columns = basic_info.xpath(".//b")

        for header_element in columns:
            key = header_element.xpath("text()").get().strip(": ")
            value = (
                header_element.xpath("string(following-sibling::text()[1])")
                .get()
                .strip()
            )
            if key == "Case Number":
                details[key] = value
                details["case_type"] = _case_type(value)
            elif key == "Date Filed":
                details[key] = datetime.datetime.strptime(value, "%m/%d/%Y").date()
            else:
                details[key] = value

        # Tallies
        tallies = []

        for tally_element in tally_elements:
            tally_details = {}
            columns = tally_element.xpath(".//b")

            for header_element in columns:
                key = header_element.xpath("text()").get().strip(": ")
                value = (
                    header_element.xpath("string(following-sibling::text()[1])")
                    .get()
                    .strip()
                )
                tally_details[key] = value

            tallies.append(tally_details)

        details["tallies"] = tallies

        related_documents = []
        if "Related Documents data is not available" not in response.text:
            related_document_header = response.xpath(
                ".//h2[text()='Related Documents']"
            )
            if related_document_header:
                document_list = related_document_header[0].xpath(
                    "following-sibling::ul[1]"
                )
                for doc_link in document_list.xpath(".//a"):
                    related_documents.append(
                        {
                            "name": doc_link.xpath("string()").get(),
                            "url": doc_link.attrib.get("href"),
                        }
                    )

        details["related_documents"] = related_documents

        allegations = []
        if "Allegations data is not available" not in response.text:
            allegation_list = response.xpath(
                ".//h2[text()='Allegations']/following-sibling::ul[1]"
            )

            for item in allegation_list.xpath(".//li"):
                allegations.append({"allegation": item.xpath("text()").get()})

        details["allegations"] = allegations

        # Participants
        participants = []
        if "Participants data is not available" not in response.text:
            participant_table = response.xpath(
                "//table[starts-with(@class, 'Participant')]/tbody"
            )

            for row in participant_table.xpath("./tr[not(td[@colspan=3])]"):
                participant_entry = {}

                participant, address, phone = row.xpath("./td")

                participant_entry["type"] = (
                    participant.xpath("./b/text()").get().strip()
                )
                participant_text = [
                    text.strip()
                    if (text := br.xpath("following-sibling::text()").get())
                    else ""
                    for br in participant.xpath("./br")
                ]
                participant_entry["subtype"], *participant_text = participant_text
                participant_entry["participant"] = "\n".join(participant_text).strip()
                participant_entry["address"] = "\n".join(
                    line.strip() for line in address.xpath("./text()").getall()
                ).strip()
                participant_entry["phone_number"] = (
                    phone.xpath("./text()").get().strip()
                )

                participants.append(participant_entry)

        details["participants"] = participants

        # Related Cases
        details["related cases"] = [
            {"related_case_number": case_number.get()}
            for case_number in response.xpath(
                "//table[starts-with(@class, 'related-case')]/tbody//a/text()"
            )
        ]

        case_number = details["Case Number"]

        called_dockets = False
        if "Docket Activity data is not available" not in response.text:
            (docket_table,) = response.xpath(
                "//div[@id='case_docket_activity_data']/table/tbody"
            )
            docket = _parse_docket_table(docket_table)

            if len(docket) < 10:
                details["docket"] = docket

            else:

                yield scrapy.http.JsonRequest(
                    f"https://www.nlrb.gov/sort-case-decisions-cp/{case_number}/ds_activity+desc/case-docket-activity/ds-activity-date/1?_wrapper_format=drupal_ajax&_wrapper_format=drupal_ajax",  # noqa
                    callback=self.parse_docket,
                    cb_kwargs={"item": details},
                )
                called_dockets = True

        if not called_dockets:
            yield scrapy.FormRequest(
                url="https://www.nlrb.gov/advanced-search",
                formdata={
                    "foia_report_type": "cases_and_decisions",
                    "cases_and_decisions_cboxes[close_method]": "close_method",
                    "cases_and_decisions_cboxes[employees]": "employees",
                    "cases_and_decisions_cboxes[union]": "union",
                    "cases_and_decisions_cboxes[unit_description]": "unit_description",
                    "cases_and_decisions_cboxes[voters]": "voters",
                    "cases_and_decisions_cboxes[date_closed]": "date_closed",
                    "cases_and_decisions_cboxes[city]": "city",
                    "cases_and_decisions_cboxes[state]": "state",
                    "cases_and_decisions_cboxes[case]": "case",
                    "search_term": f'"{case_number}"',
                },
                method="GET",
                callback=self.parse_advanced_search,
                cb_kwargs={"item": details},
            )

    def parse_advanced_search(self, response, item):

        result_table = response.xpath(
            "//table[contains(@class, 'foia-advanced-search-results-table-two')]"
        )

        keys = result_table.xpath("./thead/tr/th/text()").getall()

        rows = result_table.xpath("./tbody/tr")
        assert len(rows) == 1
        row = rows[0]

        result = {
            key: td.xpath(".//text()").get() for key, td in zip(keys, row.xpath("./td"))
        }
        assert item["Case Number"] == result.pop("Case Number")
        item.update(result)

        return item

    def parse_docket(self, response, item):

        page_snippet = scrapy.selector.Selector(text=response.json()[3]["data"])

        (docket_table,) = page_snippet.xpath("//table/tbody")
        item["docket"] = _parse_docket_table(docket_table)

        case_number = item["Case Number"]

        yield scrapy.FormRequest(
            url="https://www.nlrb.gov/advanced-search",
            formdata={
                "foia_report_type": "cases_and_decisions",
                "cases_and_decisions_cboxes[close_method]": "close_method",
                "cases_and_decisions_cboxes[employees]": "employees",
                "cases_and_decisions_cboxes[union]": "union",
                "cases_and_decisions_cboxes[unit_description]": "unit_description",
                "cases_and_decisions_cboxes[voters]": "voters",
                "cases_and_decisions_cboxes[date_closed]": "date_closed",
                "cases_and_decisions_cboxes[city]": "city",
                "cases_and_decisions_cboxes[state]": "state",
                "cases_and_decisions_cboxes[case]": "case",
                "search_term": f'"{case_number}"',
            },
            method="GET",
            callback=self.parse_advanced_search,
            cb_kwargs={"item": item},
        )


def _case_type(case_number):
    if "-RC-" in case_number:
        case_type = "RC"
    elif "-RM-" in case_number:
        case_type = "RM"
    elif "-RD-" in case_number:
        case_type = "RD"
    elif "-UD-" in case_number:
        case_type = "UD"
    elif "-UC-" in case_number:
        case_type = "UC"
    elif "-CA-" in case_number:
        case_type = "CA"
    elif "-CD-" in case_number:
        case_type = "CD"
    elif "-CC-" in case_number:
        case_type = "CC"
    elif "-CB-" in case_number:
        case_type = "CB"
    elif "-CE-" in case_number:
        case_type = "CE"
    elif "-CP-" in case_number:
        case_type = "CP"
    elif "-CG-" in case_number:
        case_type = "CG"
    elif "-AC-" in case_number:
        case_type = "AC"
    elif "-WH-" in case_number:
        case_type = "WH"
    else:
        raise ValueError("Unknown Case Type")

    return case_type


def _parse_docket_table(docket_table):
    docket = []
    for row in docket_table.xpath("./tr"):
        docket_entry = {}

        date, document, party = row.xpath("./td")

        date_str = date.xpath("text()").get().strip()
        if date_str == "pre 2010":
            docket_entry["date"] = None
        else:
            docket_entry["date"] = datetime.datetime.strptime(
                date_str, "%m/%d/%Y"
            ).date()

        document_link = document.xpath("./a")
        if document_link:
            docket_entry["document"] = document_link.xpath("text()").get().strip()
            docket_entry["url"] = document_link.attrib.get("href")
        else:
            docket_entry["document"] = document.xpath("text()").get().strip().strip("*")

        docket_entry["issued_by/filed_by"] = (
            text.strip() if (text := party.xpath("text()").get()) else None
        )

        docket.append(docket_entry)

    return docket
