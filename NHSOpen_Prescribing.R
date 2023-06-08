library(pacman)
p_load(here,
       tidyverse,
       magrittr,
       httr,
       glue,
       jsonlite,
       RColorBrewer)
devtools::source_gist("https://gist.github.com/gbrlrgrs/fec0190eb7b5884ca713c61651476b00")

# simple example ----------------------------------------------------------
strURL <- "https://opendata.nhsbsa.net/api/3/action/datastore_search_sql?resource_id=EPD_201401&sql="
strSQL <- paste("SELECT *",
                "FROM `EPD_201401`",
                "WHERE BNF_CHEMICAL_SUBSTANCE = '0601023AG'")

jsonRet <- paste0(strURL, strSQL) |> 
  URLencode() |> 
  jsonlite::fromJSON()

tblDap <- jsonRet$result$result$records |> 
  dplyr::as_tibble() |> 
  dplyr::arrange(-TOTAL_QUANTITY)

tblDap

# more complicated request [long way]: API serves zipped CSV; download; unzip; summarise -------------
strURL <- "https://opendata.nhsbsa.net/api/3/action/datastore_search_sql?resource_id=EPD_201401&sql="
strSQL <- paste("SELECT YEAR_MONTH, BNF_CHEMICAL_SUBSTANCE, CHEMICAL_SUBSTANCE_BNF_DESCR, BNF_CODE,",
                "BNF_DESCRIPTION, BNF_CHAPTER_PLUS_CODE, TOTAL_QUANTITY, NIC, ACTUAL_COST, ADQUSAGE",
                "FROM `EPD_201401`",
                "WHERE BNF_CHEMICAL_SUBSTANCE LIKE '060102%'")

jsonRet <- paste0(strURL, strSQL) |> 
  URLencode() |> 
  jsonlite::fromJSON()

gz <- tempfile()
httr::GET(url    = jsonRet$result$gc_urls$url,
          config = httr::write_disk(gz),
          httr::progress())

tblCSV <- gzfile(gz) |> 
  read.csv() |> 
  dplyr::as_tibble()

tblCSV |> 
  dplyr::group_by(BNF_CHEMICAL_SUBSTANCE, CHEMICAL_SUBSTANCE_BNF_DESCR, BNF_CODE, BNF_DESCRIPTION, BNF_CHAPTER_PLUS_CODE) |> 
  dplyr::summarise(Quantity = sum(TOTAL_QUANTITY),
                   NIC      = sum(NIC),
                   Cost     = sum(ACTUAL_COST),
                   ADQ      = mean(ADQUSAGE)) |> 
  dplyr::ungroup() |> 
  dplyr::select(BNF_CODE, CHEMICAL_SUBSTANCE_BNF_DESCR, BNF_DESCRIPTION, Quantity, NIC, Cost, ADQ)

tblCSV

# more complicated request [quick way]: do aggregating within SQL of API call ------------------------
strURL <- "https://opendata.nhsbsa.net/api/3/action/datastore_search_sql?resource_id=EPD_201401&sql="
strSQL <- paste("SELECT YEAR_MONTH, BNF_CHEMICAL_SUBSTANCE, CHEMICAL_SUBSTANCE_BNF_DESCR,",
                "BNF_CODE, BNF_DESCRIPTION, BNF_CHAPTER_PLUS_CODE,",
                "sum(TOTAL_QUANTITY) AS Quantity, sum(NIC) AS NIC,",
                "sum(ACTUAL_COST) AS Cost, avg(ADQUSAGE) AS ADQ",
                "FROM `EPD_201401`",
                "WHERE BNF_CHEMICAL_SUBSTANCE LIKE '060102%'",
                "GROUP BY YEAR_MONTH, BNF_CHEMICAL_SUBSTANCE, CHEMICAL_SUBSTANCE_BNF_DESCR,",
                "BNF_CODE, BNF_DESCRIPTION, BNF_CHAPTER_PLUS_CODE")

jsonRet <- paste0(strURL, strSQL) |> 
  URLencode() |> 
  jsonlite::fromJSON()

tblSQL <- jsonRet$result$result$records |> 
  dplyr::as_tibble() |> 
  dplyr::arrange(BNF_CODE)

tblSQL |> 
  dplyr::select(BNF_CODE, CHEMICAL_SUBSTANCE_BNF_DESCR, BNF_DESCRIPTION, Quantity, NIC, Cost, ADQ)


# putting it all into a flexible function ---------------------------------
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

## example usage ---------------------------------------------------------
fnNHSOpenData(dataset    = "EPD",
              Year       = 2014,
              Month      = 1,
              strSelect  = paste("YEAR_MONTH, BNF_CHEMICAL_SUBSTANCE, CHEMICAL_SUBSTANCE_BNF_DESCR, BNF_CODE, BNF_DESCRIPTION,",
                                 "BNF_CHAPTER_PLUS_CODE, sum(TOTAL_QUANTITY) AS Quantity, sum(NIC) AS NIC,",
                                 "sum(ACTUAL_COST) AS Cost, avg(ADQUSAGE) AS ADQ"),
              strWhere   = "BNF_CHEMICAL_SUBSTANCE LIKE '060102%'",
              strGroupBy = paste("YEAR_MONTH, BNF_CHEMICAL_SUBSTANCE, CHEMICAL_SUBSTANCE_BNF_DESCR, BNF_CODE,",
                                 "BNF_DESCRIPTION, BNF_CHAPTER_PLUS_CODE")) -> tblDM

fnNHSOpenData(dataset    = "PCA",
              Year       = 2022,
              Month      = 12,
              strWhere   = "BNF_CHEMICAL_SUBSTANCE like '%flozin%'") -> tblPCA_SGLT2

fnNHSOpenData(dataset    = "SCMD",
              Year       = 2020,
              Month      = 12,
              strWhere   = paste("VMP_PRODUCT_NAME like",
                                 c("'Adalimumab%'", "'Certolizumab%'", "'Etanercept%'", "'Golimumab%'", "'Infliximab%'"),
                                 collapse = " OR ")) -> tblSCMD_aTNF

crossing(Year       = 2014:2023,
         Month      = 1:12,
         dataset    = "EPD",
         strSelect  = paste("YEAR_MONTH, BNF_CHEMICAL_SUBSTANCE, CHEMICAL_SUBSTANCE_BNF_DESCR, BNF_CODE, BNF_DESCRIPTION, BNF_CHAPTER_PLUS_CODE,",
                            "sum(TOTAL_QUANTITY) AS Quantity, sum(NIC) AS NIC, sum(ACTUAL_COST) AS Cost, avg(ADQUSAGE) AS ADQ"),
         strWhere   = "BNF_CHEMICAL_SUBSTANCE LIKE '060102%'",
         strGroupBy = "YEAR_MONTH, BNF_CHEMICAL_SUBSTANCE, CHEMICAL_SUBSTANCE_BNF_DESCR, BNF_CODE, BNF_DESCRIPTION, BNF_CHAPTER_PLUS_CODE") |> 
  filter(Year < 2023 | Month < 4) |>
  pmap(.f = fnNHSOpenData,
       .progress = T) |> 
  list_rbind() -> tblAllDM

tblAllDM %<>%
  mutate(oral  = str_detect(BNF_DESCRIPTION, "tablet"),
         YM    = parse_date(as.character(YEAR_MONTH), format="%Y%m"),
         class = case_when(
           str_detect(CHEMICAL_SUBSTANCE_BNF_DESCR, "/")       ~ "Comb",
           str_detect(CHEMICAL_SUBSTANCE_BNF_DESCR, "Metform") ~ "Met",
           str_detect(CHEMICAL_SUBSTANCE_BNF_DESCR, "Acarbos") ~ "Aca",
           str_detect(CHEMICAL_SUBSTANCE_BNF_DESCR, "glinide") ~ "Meg",
           str_detect(CHEMICAL_SUBSTANCE_BNF_DESCR, "azone")   ~ "TZD",
           str_detect(CHEMICAL_SUBSTANCE_BNF_DESCR, "gliptin") ~ "DPP-4",
           str_detect(CHEMICAL_SUBSTANCE_BNF_DESCR, "flozin")  ~ "SGLT-2",
           str_detect(CHEMICAL_SUBSTANCE_BNF_DESCR, "tide")    ~ "GLP-1",
           str_ends(CHEMICAL_SUBSTANCE_BNF_DESCR,   "ide")     ~ "SU"))

ts <- 20
fnSetGGPlotThemeGR(lw = 0.5, ts)

tblAllDM %>% 
  group_by(class, oral, YM) %>% 
  summarise(across(c(Quantity, NIC, Cost), sum), .groups="drop") %>% 
  group_by(YM) %>% 
  mutate(across(c(Quantity, NIC, Cost), ~./sum(.), .names="p{.col}")) %>% 
  arrange(YM, -pCost) %>%
  ggplot(aes(fill = class, y = pCost, x = YM)) +
  geom_bar(position="stack", stat="identity", width=25, alpha = 1) +
  scale_fill_brewer(palette="BrBG", name = "Drug class\n") +
  scale_x_date(name        = NULL,
               expand      = c(0,0),
               limits      = c(as.Date("2015-12-15"), NA),
               date_breaks = "1 year",
               minor_breaks = NULL,
               date_labels = "%Y") +
  scale_y_continuous(name   = "Proportion of total prescribing cost",
                     breaks = 0:10/10,
                     minor_breaks = NULL,
                     expand = c(0,0)) +
  theme(rect              = element_rect(fill = "transparent", linetype = 0),
        plot.margin       = margin(1,1,1,1, "cm"),
        panel.border      = element_blank(),
        axis.text.x       = element_text(size = ts, margin = margin(0.5,0,0,0, "cm")),
        axis.text.y       = element_text(size = ts, margin = margin(0,0.5,0,0, "cm")),
        axis.title.y.left = element_text(size = ts, face = "bold", margin = margin(0,0.5,0,0, "cm")),
        axis.ticks.length = unit(.25, "cm"),
        legend.title      = element_text(size = ts, face = "bold"),
        legend.text       = element_text(size = ts),
        legend.key.width  = unit(x = 1, units = "cm"),
        legend.key.height = unit(x = 1, units = "cm"),
        legend.position   = "right",
        legend.margin     = margin(0,0,0,1, "cm"))

fnSaveGG(strName = "EPD_DiabPres", cmW = 33.867, cmH = 19.05, typ = "svg")
