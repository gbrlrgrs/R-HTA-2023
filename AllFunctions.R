library(pacman)
p_load(here,
       tidyverse,
       magrittr,
       httr,
       glue,
       jsonlite,
       RColorBrewer)

fnNHSOpenData <- function(dataset = c("PCA", "EPD", "SCMD"), Year, Month, strSelect = "*", strWhere = "", strGroupBy = "") {
  
  strURL <- "https://opendata.nhsbsa.net/api/3/action/datastore_search_sql?resource_id={res}&sql="
  if (dataset=="SCMD") dataset <- paste0("SCMD_", c("FINAL", "WIP", "PROVISIONAL"))
  res <- paste0(dataset, "_", Year, sprintf("%02d", Month))
  strSQL <- paste("SELECT", strSelect, "FROM `{res}`")
  if (strWhere != "") strSQL <- paste(strSQL, "WHERE", strWhere)
  if (strGroupBy != "") strSQL <- paste(strSQL, "GROUP BY", strGroupBy)
  
  url <- glue(strURL, strSQL) |> 
    URLencode()
  
  for (i in 1:length(url)) {
    suppressWarnings(xx <- tryCatch(fromJSON(url[i]), error=function(e) e))
    if (!inherits(xx, "error")) break
  }
  
  if (!is.null(xx$result$records_truncated)) {
    gz <- tempfile()
    httr::GET(url    = xx$result$gc_urls$url,
              config = httr::write_disk(gz))
    tibble(Dataset = res[i]) |>
      bind_cols(gzfile(gz) |> 
                  read.csv() |> 
                  dplyr::as_tibble())
  } else {
    tibble(Dataset = res[i]) |> 
      bind_cols(xx$result$result$records |> 
                  as_tibble())
  }
}
fnReadLifeTable <- function(path, sheet, range, yr) {
  suppressMessages(
    read_excel(path, sheet, range) %>% 
      mutate(yr = sheet)
  )
}
fnRateToProb <- function(r, t=1) 1 - exp(-r * t)
fnProbToRate <- function(p, t=1) -log(1 - p) / t
fnProbToProb <- function(p, t=1) 1 - (1 - p) ^ t
fnONSLifeTable<- function(years       = 1980:2018,
                          ages        = 0:100,
                          sexes       = c("Male", "Female"),
                          countries   = c("UK", "Eng", "Sco", "Wal", "EngWal"),
                          spans       = c("3-year", "single year"),
                          extrapolate = FALSE,
                          maxExtrap   = 120,
                          widthExtrap = 10) {
  countries <- match.arg(countries)
  spans <- match.arg(spans)
  url <- paste0(switch(spans,
                       "3-year"      = "https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fbirthsdeathsandmarriages%2flifeexpectancies%2fdatasets%2fnationallifetables",
                       "single year" = "https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fbirthsdeathsandmarriages%2flifeexpectancies%2fdatasets%2fsingleyearlifetablesuk1980to2018%2fsingleyearlifetables"),
                switch(spans,
                       "3-year"      = switch(countries,
                                              "Eng"    = "englandreferencetables%2fcurrent/nationallifetables3yreng.xlsx",
                                              "Sco"    = "scotlandreferencetables%2fcurrent/nationallifetables3yrsco.xlsx",
                                              "Wal"    = "walesreferencetables%2fcurrent/nationallifetables3yrwal.xlsx",
                                              "EngWal" = "englandandwalesreferencetables%2fcurrent/nationallifetables3yrew.xlsx",
                                              "unitedkingdomreferencetables%2fcurrent/nationallifetables3yruk.xlsx"),
                       "single year" = switch(countries,
                                              "Eng"    = "england/singleyearlifetablese.xlsx",
                                              "Sco"    = "scotland/singleyearlifetabless.xlsx",
                                              "Wal"    = "wales/singleyearlifetablesw1.xlsx",
                                              "EngWal" = "englandandwales/singleyearlifetablesew.xlsx",
                                              "uk/singleyearlifetablesuk.xlsx")))
  GET(url, write_disk(tmp <- tempfile(fileext = ".xlsx")))
  tblYrs <- tibble(path  = tmp,
                   sheet = tmp %>% 
                     excel_sheets() %>%
                     str_subset(pattern = if_else(spans=="3-year", "\\d{4}-\\d{4}", "\\d{4}")),
                   range = "A6:M107")
  tblLT <- tblYrs %>%
    filter(substr(sheet, 1, 4) %in% years) %>%
    pmap_dfr(fnReadLifeTable)
  tblLT %<>%
    dplyr::select(!c(...7, age...8)) %>%
    rename(age = age...1) %>%
    pivot_longer(cols      = !c(age, yr),
                 names_to  = "tmp",
                 values_to = "val") %>%
    separate(col  = tmp,
             into = c("par", "sex")) %>% 
    mutate(sex       = ifelse(as.numeric(sex)<7, "Male", "Female"),
           year_from = as.numeric(substr(yr, 1, 4)),
           year_to   = year_from + if_else(spans=="3-year", 2, 0)) %>% 
    pivot_wider(id_cols     = c(age, year_from, year_to, sex),
                values_from = val,
                names_from  = par) %>%
    rowwise() %>%
    mutate(lx = ifelse(min(ages)==0, lx, NA),
           dx = ifelse(min(ages)==0, dx, NA)) %>% 
    filter(year_from %in% years,
           age       %in% ages,
           sex       %in% sexes) %>%
    arrange(-year_from, sex, age) %>%
    group_by(sex, year_from, year_to)
  
  if (extrapolate) {
    maxA <- max(tblLT$age)
    tblLT %<>%
      slice_tail(n=widthExtrap) %>%
      nest() %>%
      mutate(lm_obj  = map(data, ~ lm(log(mx)~log(age), data=.x)),
             preddat = list(tibble(age = (maxA+1):(maxExtrap+1))),
             pred    = map2(lm_obj, preddat, predict)) %>%
      unnest(c(preddat, pred)) %>%
      mutate(mx = exp(pred)) %>%
      select(!c(data, lm_obj, pred)) %>%
      bind_rows(tblLT)
  }
  if (extrapolate | min(ages)>0) {
    tblLT %<>%
      arrange(-year_from, sex, age) %>% 
      mutate(qx  = coalesce(qx, fnRateToProb(mx)),
             llx = if_else(row_number()==1, 100000, lag(100000*cumprod(1-qx), 1)),
             lx  = coalesce(lx, llx),
             dx  = coalesce(dx, lx*qx),
             Lx  = (lx+lead(lx,1))/2,
             Tx  = rev(cumsum(rev(coalesce(Lx, 0)))),
             Ex  = Tx/lx,
             ex  = coalesce(ex, Ex)) %>%
      filter(age<=maxExtrap) %>%
      dplyr::select(!c(llx, Lx, Tx, Ex))
  }
  tblLT %>%
    ungroup()
}

fnSurvFromLT <- function(LT, Yr, Sex, minAge, maxAge, CyclesPerYr = 1, probVar = qx) {
  if (missing(Yr)) {Yr <- LT %>% distinct(year_from) %>% pull()}
  if (missing(Sex)) {Sex <- LT %>% distinct(sex) %>% pull()}
  if (missing(minAge)) {minAge <- min(LT$age)}
  if (missing(maxAge)) {maxAge <- max(LT$age)}
  probVar <- enquo(probVar)
  numCycles <- (maxAge-minAge)*CyclesPerYr+1
  tibble(Cycle = 0:(numCycles-1)) %>%
    crossing(Year_from = Yr, Sex) %>%
    mutate(Yrs = Cycle / CyclesPerYr,
           Age = minAge + Yrs) %>%
    left_join(LT %>%
                filter(year_from %in% Yr,
                       sex  %in% Sex,
                       age  >= minAge,
                       age  <= maxAge) %>%
                select(age, sex, year_from, !!probVar),
              by = c("Age" = "age", "Sex" = "sex", "Year_from" = "year_from")) %>%
    mutate(qxPC = fnProbToProb(!!probVar, 1/CyclesPerYr)) %>%
    group_by(Sex, Year_from) %>%
    fill(qxPC) %>%
    mutate(St      = if_else(Cycle==0, 1, lag(1*cumprod(1-qxPC), 1)),
           Year_to = Year_from + 2) %>%
    select(Year_from, Year_to, Sex, Cycle, Yrs, Age, TP = qxPC, St) %>%
    arrange(Year_from, Sex, Cycle)
}
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