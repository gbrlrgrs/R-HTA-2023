library(pacman)
p_load(here,
       tidyverse,
       magrittr,
       janitor,
       httr,
       data.table)

# functions ----

fnGetZipCSV <- function(url, csv, subzip = "") {
  print("Downloading...")
  zip <- tempfile()
  GET(url    = url,
      config = write_disk(zip),
      progress())
  if (subzip != "") {
    print("Unzipping subzip...")
    zip <- unzip(zipfile   = zip,
                 files     = subzip,
                 exdir     = dirname(tempdir()),
                 junkpaths = T)
  }
  print("Unzipping...")
  csv <- unzip(zipfile   = zip,
               files     = csv,
               exdir     = dirname(tempdir()),
               junkpaths = T)
  unlink(zip)
  fread(file = csv)
}

fnParseRefCostsCSV <- function(csv, Yr, blnXS = TRUE) {
  print("Parsing...")
  csv %<>%
    clean_names(case = "upper_camel") %>%
    rename(any_of(c(CurrencyCode = "CurrenyCode",
                    Mean         = "NationalMean",
                    DeptCode     = "DepartmentCode",
                    Activity     = "TotalActivity",
                    OrgCode      = "ProviderCode"))) %>%
    mutate(across(.cols = any_of(c("Activity", "Mean", "ExpectedCost")),
                  .fns  = ~ as.numeric(na_if(as.character(.x), "*"))),
           across(.cols = any_of(c("ServiceCode")),
                  .fns  = ~ as.character(.x)),
           DeptCode = str_replace_all(DeptCode,
                                      c("TEI$|EI$"             = "EL",
                                        "TEIXS$|EI_XS$"        = "EL_XS",
                                        "TNEI_L$|NEI_L$"       = "NEL",
                                        "TNEI_L_XS$|NEI_L_XS$" = "NEL_XS",
                                        "TNEI_S$|NEI_S$"       = "NES",
                                        "TDC$"                 = "DC",
                                        "TOPROC"               = "OPROC",
                                        "\\GTCL"               = "CL",
                                        "\\GTNCL"              = "NCL"))) %>%
    select(-any_of(c("Memo1", "ScaledMff", "OrganisationName", "DepartmentName", "ServiceName", "CurrencyName"))) %>%
    add_column(Yr = Yr) %>%
    as_tibble()
  if (Yr==2008) {
    csv %<>%
      rename(any_of(c(Activity = "Fce",
                      Activity = "ActivityP1"))) %>%
      select(-any_of(c("Flag", "ActivityP2", "ActivityP3", "ActivityP4")))
    if ("BedDays" %in% names(csv)) {
      csv %<>%
        pivot_longer(cols          = contains(c("UnitCost", "Activity")),
                     names_to      = c("tmp1", "tmp2"),
                     names_pattern = "(.*)(UnitCost|Activity)") %>%
        drop_na(value) %>%
        mutate(DeptCode = paste0(DeptCode, ifelse(tmp1 == "ExcessBedDays", "_XS", "")),
               BedDays  = ifelse(tmp1 == "ExcessBedDays", NA, BedDays)) %>%
        pivot_wider(names_from = tmp2,
                    values_from = value) %>%
        select(-tmp1)
    }
    csv %<>%
      mutate(ActualCost = UnitCost * Activity)
  }
  csv %<>%
    bind_rows(tibble(OrgType = character(),
                     SupplierType = character(),
                     UnitCost = numeric(),
                     BedDays = numeric()))
}

fnDownloadParseCSV <- function(url, subzip, csv, Yr) {
  print(Yr)
  fnGetZipCSV(url    = url,
              subzip = subzip,
              csv    = csv) %!>%
    fnParseRefCostsCSV(Yr = Yr)
}

# download and parse----
tblY <- tibble(Yr     = c(2008, 2008:2021),
               url    = c("https://webarchive.nationalarchives.gov.uk/ukgwa/20130105032209mp_/http://www.dh.gov.uk/prod_consum_dh/groups/dh_digitalassets/@dh/@en/@ps/documents/digitalasset/dh_118327.zip",
                          "https://webarchive.nationalarchives.gov.uk/ukgwa/20130105032209mp_/http://www.dh.gov.uk/prod_consum_dh/groups/dh_digitalassets/@dh/@en/@ps/documents/digitalasset/dh_118327.zip",
                          "https://webarchive.nationalarchives.gov.uk/ukgwa/20200507181314mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/216656/dh_129668.zip",
                          "https://webarchive.nationalarchives.gov.uk/ukgwa/20200506054538mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/215304/dh_131170.zip",
                          "https://webarchive.nationalarchives.gov.uk/ukgwa/20200506093533mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/213071/dh_134774.zip",
                          "https://webarchive.nationalarchives.gov.uk/ukgwa/20200505005037mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/260171/1_Data.zip",
                          "https://webarchive.nationalarchives.gov.uk/ukgwa/20200506033032mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/379852/1_Data.zip",
                          "https://webarchive.nationalarchives.gov.uk/ukgwa/20200506073612mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/478291/8._Organisation_level_source_data_part_1___data__1_.zip",
                          "https://webarchive.nationalarchives.gov.uk/ukgwa/20200506094432mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/577092/Organisation_level_source_data_part_1.zip",
                          "https://webarchive.nationalarchives.gov.uk/ukgwa/20200501111106mp_/https://improvement.nhs.uk/documents/6467/201617_ReferenceCostData.zip",
                          "https://webarchive.nationalarchives.gov.uk/ukgwa/20200501111106mp_/https://improvement.nhs.uk/documents/6468/201718_reference_costs_data_and_guidance.zip",
                          "https://www.england.nhs.uk/wp-content/uploads/2020/08/Organisation_level_source_data_part_1_1.zip", #https://web.archive.org/web/20220704114720/https://www.england.nhs.uk/wp-content/uploads/2020/08/Organisation_level_source_data_part_1_1.zip
                          "https://www.england.nhs.uk/wp-content/uploads/2021/06/NCC_Schedule_1920_Org_level_Data_1-v2.zip",
                          "https://www.england.nhs.uk/wp-content/uploads/2022/07/NCC_Schedule_2021_Data_Org_level_Data_1-1.zip",
                          "https://www.england.nhs.uk/wp-content/uploads/2023/04/NCC_Schedule_2021_Data_Org_level_Data_1_v2-1.zip"), #https://web.archive.org/web/20230303120148/https://www.england.nhs.uk/wp-content/uploads/2022/07/NCC_Schedule_2021_Data_Org_level_Data_1-1.zip
               csv    = c("tbl_Admitted_Patient_Care.csv",
                          "tbl_Non-Admitted_Patient_Care.csv",
                          "1 Data.csv",
                          "1 Data.csv",
                          "1 Data.csv",
                          "1 Data.csv",
                          "1 Data.csv",
                          "1 Data.csv",
                          "1a Data.csv",
                          "csv061117NoMff (1).csv",
                          "1 - Data.csv",
                          "1 Data V2.csv",
                          "NCC_Schedule_1920_Org_level_Data_1 v2.csv",
                          "NCC_Schedule_2021_Data_Org_level_Data_1.csv",
                          "NCC_Schedule_2021_Data_Org_level_Data_1_v2/NCC_Schedule_FY21-22_Org_level_Data_1_v2.csv"),
               subzip = c("",
                          "",
                          "",
                          "",
                          "",
                          "",
                          "",
                          "",
                          "",
                          "8 - Organisation level source data part 1.zip",
                          "7 - Organisation level source data part 1.zip",
                          "",
                          "",
                          "",
                          "")) 

tblD <- tblY %>%
  filter(between(Yr, 2008, 2021)) %>% # can adjust this filter to download particular ranges of years
  pmap(.f        = fnDownloadParseCSV,
       .progress = T) %>%
  list_rbind()

tblD %<>%
  filter(OrgType == "TRUST" | OrgType == "" | is.na(OrgType),
         SupplierType == "OWN" | SupplierType == "" | is.na(SupplierType))

# combine inlier beddays with excess beddays
tblD %<>%
  left_join(tblD %>%
              filter(DeptCode == "EL" | DeptCode == "NEL") %>%
              left_join(tblD %>%
                          filter(str_ends(DeptCode, "_XS"),
                                 ActualCost > 0) %>%
                          mutate(DeptCode = str_remove(DeptCode, "_XS")) %>%
                          rename_with(~paste0(., "_XS"), .cols = c("UnitCost", "Activity", "ActualCost")) %>%
                          select(-any_of(c("BedDays", "MappingPot", "Mean", "ExpectedCost"))),
                        by = c("OrgCode", "DeptCode", "ServiceCode", "CurrencyCode", "Yr")) %>%
              mutate(across(ends_with("_XS"), ~replace_na(., 0))) %>%
              select(-any_of(c("MappingPot"))) %>%
              mutate(TotalCost_wXS = ActualCost + ActualCost_XS,
                     BedDays_wXS   = BedDays + Activity_XS,
                     UnitCost_wXS  = TotalCost_wXS / Activity) %>%
              select(c(ends_with("wXS"), "OrgCode", "DeptCode", "ServiceCode", "CurrencyCode", "Yr")),
            by = c("OrgCode", "DeptCode", "ServiceCode", "CurrencyCode", "Yr")) %>%
  bind_rows(tibble(TotalCost_wXS = numeric(),
                   BedDays_wXS = numeric(),
                   UnitCost_wXS = numeric())) %>% 
  mutate(UnitCost      = coalesce(UnitCost, ActualCost / Activity),
         TotalCost_wXS = coalesce(TotalCost_wXS, ActualCost),
         BedDays_wXS   = coalesce(BedDays_wXS,   BedDays),
         UnitCost_wXS  = coalesce(UnitCost_wXS,  UnitCost))

tblD %>%
  fwrite(file = here("AllRefCostsData.csv")) # store local copy of full dataset


## Childbirth trends ---------------------------------------------------------------------------
p_load(viridis)

VagCodes   <- c("NZ0[12]", "NZ[34]", "NZ1[12]") %>% paste(collapse="|")
CaesCodes  <- c("NZ03", "NZ[5]", "NZ1[345]") %>% paste(collapse="|")
BirthCodes <- c(VagCodes, CaesCodes) %>% paste(collapse="|")

dd <- tblD %>%
  filter(DeptCode %in% c("EL", "NEL", "NES", "DC"),
         str_starts(CurrencyCode, BirthCodes)) %>%
  mutate(CS = ifelse(str_starts(CurrencyCode, CaesCodes), "Caesarean", "Vaginal"),
         EL = ifelse(str_starts(DeptCode, "N"), "Nonelective", "Elective")) %>%
  drop_na(Activity, ActualCost) %>%
  group_by(Yr, OrgCode, CS) %>%
  summarise(Activity     = sum(Activity, na.rm = T),
            MeanCost     = sum(ActualCost, na.rm = T) / sum(Activity, na.rm = T),
            MeanCost_wXS = sum(TotalCost_wXS, na.rm = T) / sum(Activity, na.rm = T)) %>%
  filter(Activity > 9) 

devtools::source_gist("https://gist.github.com/gbrlrgrs/fec0190eb7b5884ca713c61651476b00")
ts <- 20
lw <- 0.5
fnSetGGPlotThemeGR(lw, ts)

dd %>%
  pivot_wider(names_from  = CS,
              values_from = c(Activity, MeanCost, MeanCost_wXS)) %>%
  drop_na() %>%
  group_by(Yr, OrgCode) %>%
  mutate(pCS = sum(Activity_Caesarean) / sum(Activity_Caesarean + Activity_Vaginal)) %>%
  ggplot(aes(x=Yr, y=pCS)) +
  geom_path(aes(colour = OrgCode), show.legend = FALSE) +
  geom_point(aes(colour = OrgCode, size = Activity_Caesarean + Activity_Vaginal), shape = 21, show.legend = FALSE) +
  geom_path(data = . %>%
              group_by(Yr) %>%
              summarise(mean = sum(Activity_Caesarean) / sum(Activity_Caesarean + Activity_Vaginal)),
            aes(y=mean),
            colour = "black",
            size   = 1) +
  scale_colour_viridis(discrete=T, option = "turbo", alpha = 0.4) +
  scale_x_continuous(limits       = c(2008, 2021),
                     breaks       = 2000:2021,
                     minor_breaks = NULL,
                     name         = "Year")+
  scale_y_continuous(limits       = c(0, 0.6),
                     breaks       = 0:10/10,
                     minor_breaks = 0:100/100,
                     expand       = c(0,0),
                     name         = "Proportion of births by Caesarean") +
  theme(plot.margin       = margin(1,1,1,1, "cm"),
        axis.text.x       = element_text(size = ts, margin = margin(0.5,0,0,0, "cm")),
        axis.text.y       = element_text(size = ts, margin = margin(0,0.5,0,0, "cm")),
        axis.title.x      = element_text(size = ts, margin = margin(0.5,0,0,0, "cm")),
        axis.title.y      = element_text(size = ts, margin = margin(0,0.5,0,0, "cm")),
        axis.ticks.length = unit(.25, "cm"))

fnSaveGG(strName = "CSect", cmW = 33.867, cmH = 19.05, typ = "svg")
