library(pacman)
p_load(here,
       xml2,
       httr,
       rvest,
       jsonlite,
       tidyverse,
       magrittr,
       glue,
       data.table,
       readxl)

# needs an API key; you can apply for one at https://isd.digital.nhs.uk/trud/user/guest/group/0/home
myAPIkey <- "" # insert your API key here
source(here("myTRUD_API.R")) # only works for GR

tblReleases <- fromJSON(glue("https://isd.digital.nhs.uk/trud/api/v1/keys/{myAPIkey}/items/24/releases")) %>%
  extract2("releases") %>%
  as_tibble()

urlLatest <- tblReleases %>%
  slice_head(n=1) %>%
  pull(archiveFileUrl)

zip <- tempfile()
GET(url    = urlLatest,
    config = write_disk(zip),
    progress())

zip <- unzip(zipfile   = zip,
             exdir     = dirname(tempdir()),
             junkpaths = T)

fnGetXML <- function(fileLst, filSubst, xpath, IDvar, NMvar = NULL) {
  xmlFull <- fileLst %>%
    str_subset(filSubst) %>%
    read_xml()
  
  xmlChild = xml_find_all(xmlFull, xpath) %>%
    xml_children()
  
  tblOut <- tibble(x = xmlChild %>%
                     html_name(),
                   y = xmlChild %>%
                     xml_text()) %>%
    mutate(ID = ifelse(x==IDvar, y, NA)) %>%
    fill(ID) %>%
    filter(x != IDvar) %>%
    pivot_wider(names_from = x, values_from = y) %>%
    rename(!!IDvar := ID)
  
  if (!is.null(NMvar)) {
    tblOut %<>%
      rename(!!NMvar := NM)
  }
  
  return(tblOut)
}

tblX <- tibble(fileLst  = list(zip),
               filSubst = c("f_ampp2", "f_ampp2", "f_vmpp2", "f_vmp2", "f_vtm2"),
               xpath    = c(".//AMPPS/AMPP", ".//MEDICINAL_PRODUCT_PRICE/PRICE_INFO", ".//VMPPS/VMPP", ".//VMPS/VMP", ".//VTM"),
               IDvar    = c("APPID", "APPID", "VPPID", "VPID", "VTMID"),
               NMvar    = c("AMPPname", list(NULL), "VMPPname", "VMPname", "VTMname"))

lst <- tblX %>%
  pmap(.f        = fnGetXML,
       .progress = TRUE)

tblDMD <- lst[[1]] %>%
  left_join(lst[[2]] %>%
              mutate(PRICE     = na_if(PRICE, "0"),
                     CurrPrice = coalesce(PRICE, PRICE_PREV))) %>%
  left_join(lst[[3]] %>%
              select(-INVALID, -COMBPACKCD)) %>%
  left_join(lst[[4]] %>%
              select(-INVALID)) %>%
  left_join(lst[[5]] %>%
              select(-INVALID, -ABBREVNM)) %>%
  select(VTMID, VTMname, VPID, VMPname, VPPID, VMPPname, APPID, AMPPname, QTYVAL, VTMID, DISCCD, PRICE, PRICEDT, PRICE_PREV, CurrPrice)
rm(lst)


# Get BNF data to join ----------------------------------------------------

zip2 <- tempfile()
GET(url    = "https://www.nhsbsa.nhs.uk/sites/default/files/2023-04/BNF%20Snomed%20Mapping%20data%2020230420.zip",
    config = write_disk(zip2),
    progress())

zip2 <- unzip(zipfile   = zip2,
              exdir     = dirname(tempdir()),
              junkpaths = T)

tblBNF <- read_xlsx(zip2)

tblDMD %<>%
  left_join(tblBNF,
            by = join_by(APPID == `SNOMED Code`))


# example usage -------------------------------------------------------------------------------

tblDM <- tblDMD %>%
  filter(str_starts(`BNF Code`, "060102")) %>% 
  mutate(str_extract(VMPname, "(\\d+\\.?\\d*)(m(?:icro)?g(?:ram)?(?:s)?)", group = c(1,2)) %>% as_tibble(.name_repair = ~ c("strength", "unit")),
         dosesize = str_extract(VMPname, "(\\d*\\.?\\d*)ml", group = 1) %>% replace(., .=="", 1) %>% as.numeric(),
         volume   = str_extract(VMPname, " (\\d*\\.?\\d*)(?: )?ml", group = 1) %>% as.numeric(),
         across(c(dosesize, volume), ~replace_na(.x, 1)),
         doses    = trunc(volume / dosesize),
         mg = as.numeric(strength) / ifelse(str_starts(unit, "microg"), 1000, 1),
         totMg = mg * doses * as.numeric(QTYVAL),
         across(c(APPID, VPPID, VPID, VTMID), ~paste0("'", .x)))


# drug tariff -------------------------------------------------------------

tblNHS_DT <- fread("https://web.archive.org/web/20230520202515/https://www.nhsbsa.nhs.uk/sites/default/files/2023-04/Part%20VIIIA%20May%2023.xls.csv",
                   skip = "Medicine")

tblDM <- tblBNF %>%
  filter(str_starts(`BNF Code`, "0601023"),
         `VMP / VMPP/ AMP / AMPP` == "VMPP") %>%
  left_join(tblNHS_DT %>%
              mutate(across(c(`VMP Snomed Code`, `VMPP Snomed Code`), as.character)),
            by = join_by(`SNOMED Code` == `VMPP Snomed Code`))
