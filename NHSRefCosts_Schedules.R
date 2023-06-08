library(pacman)
p_load(here,
       tidyverse,
       magrittr,
       janitor,
       httr,
       readxl,
       data.table)

fnReadSchedXL <- function(Yr, fil, sht){
  read_excel(path      = fil,
             sheet     = sht,
             skip      = ifelse(Yr<2011, 8, 3),
             guess_max = 99999) %>%
    as_tibble() %>%
    clean_names(case = "upper_camel") %>%
    rename_with(~ str_replace_all(., c("Name"       = "Description",
                                       "Fce|FcEs"   = "FCEs",
                                       "TotalCosts" = "TotalCost"))) %>%
    rename_with(~ str_remove_all(., "NumberOf|NoOf|Number|No")) %>%
    mutate(across(.cols = any_of(c("FCEs", "NationalAverageUnitCost", "Procedures", "Attendances")),
                  .fns  = ~ as.numeric(na_if(as.character(.x), "*"))),
           across(.cols = any_of(c("ServiceCode")),
                  .fns  = ~ as.character(.x))) %>%
    select(-starts_with("X"))
}
pfnReadSchedXL <- possibly(fnReadSchedXL, NULL)

fnGetSched <- function(Yr, url, zip = "") {
  print(Yr)
  print("Downloading")
  GET(url, progress(), write_disk(tmp <- tempfile()))
  if (zip != "") {
    print("Unzipping")
    tmp <- unzip(zipfile = tmp,
                 files   = zip,
                 exdir   = dirname(tempdir()))
  }
  tblT <- tibble(Yr = Yr, DeptCode = c("EL", "TEI", "EI",
                                       "EL_XS", "TEIXS", "EI_XS",
                                       "NEL", "TNEI_L", "NEI_L",
                                       "NEL_XS", "TNEI_L_XS", "NEI_L_XS",
                                       "NES", "TNEI_S", "NEI_S",
                                       "DC", "TDC",
                                       "OPROC", "TOPROC",
                                       "CL", "NCL",
                                       "TCLFUSFF", "TCLFASFF", "TCLFUSNFF", "TCLFASNFF", "TCLFUMFF", "TCLFAMFF", "TCLFUMNFF", "TCLFAMNFF", "TNCLFUSFF", "TNCLFASFF", "TNCLFUSNFF", "TNCLFASNFF", "TNCLFUMFF", "TNCLFAMFF", "TNCLFUMNFF", "TNCLFAMNFF",
                                       "CLFUSFF", "CLFASFF", "CLFUSNFF", "CLFASNFF", "CLFUMFF", "CLFAMFF", "CLFUMNFF", "CLFAMNFF", "NCLFUSFF", "NCLFASFF", "NCLFUSNFF", "NCLFASNFF", "NCLFUMFF", "NCLFAMFF", "NCLFUMNFF", "NCLFAMNFF",
                                       "APC", "OP"
                                       )) # this list can be extended to extract data recorded under other department codes (i.e. in other sheets of the Excel workbooks)
  print("Extracting")
  tblT %>%
    mutate(d = pmap(.l = list(Yr, sht = DeptCode, fil = tmp),
                    .f = pfnReadSchedXL)) %>%
    filter(!is.null(d)) %>%
    unnest(d) %>%
    select(-any_of("d"))
}

tblSY <- tibble(Yr  = c(2008:2021),
               url = c("https://webarchive.nationalarchives.gov.uk/ukgwa/20130104223435mp_/http://www.dh.gov.uk/prod_consum_dh/groups/dh_digitalassets/@dh/@en/@ps/documents/digitalasset/dh_118322.xls",
                       "https://webarchive.nationalarchives.gov.uk/ukgwa/20200505201020mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/216308/dh_123455.xls",
                       "https://webarchive.nationalarchives.gov.uk/ukgwa/20200506054538mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/215298/dh_131145.xls",
                       "https://webarchive.nationalarchives.gov.uk/ukgwa/20200506093533mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/213063/NSRC01-2011-12.xls",
                       "https://webarchive.nationalarchives.gov.uk/ukgwa/20200505005037mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/397487/NSRC01_2012-13_in_13-14_format.xls",
                       "https://webarchive.nationalarchives.gov.uk/ukgwa/20200506033032mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/397469/03a_2013-14_National_Schedule_-_CF-NET_updated.xls",
                       "https://webarchive.nationalarchives.gov.uk/ukgwa/20200506073612mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/480791/2014-15_National_Schedules.xlsx",
                       "https://webarchive.nationalarchives.gov.uk/ukgwa/20200506094432mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/577084/National_schedule_of_reference_costs_-_main_schedule.xlsx",
                       "https://webarchive.nationalarchives.gov.uk/ukgwa/20200501111106mp_/https://improvement.nhs.uk/documents/6467/201617_ReferenceCostData.zip",
                       "https://webarchive.nationalarchives.gov.uk/ukgwa/20200501111106mp_/https://improvement.nhs.uk/documents/6468/201718_reference_costs_data_and_guidance.zip",
                       "https://www.england.nhs.uk/wp-content/uploads/2020/08/2_-_National_schedule_of_NHS_costs_V2.xlsx",
                       "https://www.england.nhs.uk/wp-content/uploads/2021/06/2_National_schedule_of_NHS_costs_FY19_20_V2.xlsx",
                       "https://www.england.nhs.uk/wp-content/uploads/2022/07/2_National_schedule_of_NHS_costs_FY20-21.xlsx",
                       "https://www.england.nhs.uk/wp-content/uploads/2023/04/2_National_schedule_of_NHS_costs_FY21-22_v3.xlsx"),
               zip = c("",
                       "",
                       "",
                       "",
                       "",
                       "",
                       "",
                       "",
                       "2 - National schedule of reference costs - the main schedule.xlsx",
                       "2 - National schedule of reference costs.xlsx",
                       "",
                       "",
                       "",
                       ""))
tblS <- tblSY %>%
  filter(between(Yr, 2008, 2021)) %>% # can adjust this filter to download particular ranges of years
  pmap(.f = fnGetSched,
       .progress = T) %>%
  list_rbind() %>%
  mutate(DeptCode = str_replace_all(DeptCode,
                                    c("TEI$|EI$"             = "EL",
                                      "TEIXS$|EI_XS$"        = "EL_XS",
                                      "TNEI_L$|NEI_L$"       = "NEL",
                                      "TNEI_L_XS$|NEI_L_XS$" = "NEL_XS",
                                      "TNEI_S$|NEI_S$"       = "NES",
                                      "TDC$"                 = "DC",
                                      "TOPROC"               = "OPROC",
                                      "\\GTCL"               = "CL",
                                      "\\GTNCL"              = "NCL")),
         CurrencyCode = case_when(str_detect(DeptCode, "CLFUSFF")  ~ "WF01A",
                                  str_detect(DeptCode, "CLFASFF")  ~ "WF01B",
                                  str_detect(DeptCode, "CLFUSNFF") ~ "WF01C",
                                  str_detect(DeptCode, "CLFASNFF") ~ "WF01D",
                                  str_detect(DeptCode, "CLFUMFF")  ~ "WF02A",
                                  str_detect(DeptCode, "CLFAMFF")  ~ "WF02B",
                                  str_detect(DeptCode, "CLFUMNFF") ~ "WF02C",
                                  str_detect(DeptCode, "CLFAMNFF") ~ "WF02D",
                                  TRUE                             ~ CurrencyCode),
         DeptCode = do.call(coalesce, pick(any_of(c("DepartmentCode", "DeptCode")))),
         DeptCode = str_replace_all(DeptCode, c("\\GCL.*" = "CL",
                                                "\\GNCL.*" = "NCL")))

tblS %>%
  fwrite(file = here("AllRefCostsSchedules.csv")) # store local copy of full dataset
