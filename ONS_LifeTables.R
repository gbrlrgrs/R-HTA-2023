# basic code worked through by hand --------------------------------------------
strURL <- paste0("https://www.ons.gov.uk/file?",
                 "uri=%2fpeoplepopulationandcommunity%2fbirthsdeathsandmarriages%2flifeexpectancies%2fdatasets%2fnationallifetables",
                 "englandreferencetables%2fcurrent/nationallifetables3yreng.xlsx")

tmp <- tempfile(fileext = ".xlsx")
httr::GET(strURL, httr::write_disk(tmp))

tblLT <- readxl::read_excel(path  = tmp,
                            sheet = "2018-2020",
                            range = "A6:M107")

tblLT |> 
  dplyr::select(!c(...7, age...8)) |>
  dplyr::rename(age = age...1) |>
  tidyr::pivot_longer(cols      = !age,
                      names_to  = "tmp",
                      values_to = "val") |>
  tidyr::separate(col  = tmp,
                  into = c("par", "sex")) |> 
  dplyr::mutate(sex       = ifelse(as.numeric(sex)<7, "Male", "Female")) |> 
  tidyr::pivot_wider(id_cols     = c(age, sex),
                     values_from = val,
                     names_from  = par)


# flexible, functional implementation ------------------------------------------

## This script contains 2 functions:
##
## fnONSLifeTable downloads and parses lifetables from the ONS's website
##
## example usage:
## fnONSLifeTable(years=2010:2017, ages=40:100, sexes="Female", extrapolate=TRUE, maxExtrap=110)
## 'years' represents lower bound of 3-year bracket -- e.g. '2010' returns 2010-2012
## current possible range is 1980:2018 (i.e. 1980-82 to 2018-20)
## 'countries' can be one of "UK", "Eng", "Sco", "Wal", "EngWal"; if not specified, will default to "UK"
## 'spans' can be one of "3-year", "single year"; if not specified, will default to "3-year"
## 'extrapolate' provides estimates of death probabilities at ages above the requested range (usually 100),
## assuming ln(hazard) is linear in ln(years); maxExtrap gives an upper age limit to the extrapolation
## all arguments are optional; if none are specified, fnONSLifeTable() will return all years, ages and sexes for UK
## obviously, if ONS ever move or reformat their lifetable spreadsheets, everything will be screwed...
##
## fnSurvFromLT takes a lifetable created by fnONSLifeTable and creates S(t) and transition probabilities

library(pacman)
p_load(here,
       tidyverse,
       magrittr,
       httr,
       readxl)

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

# example usage -----------------------------------------------------------
tblFem2000_2018 <- fnONSLifeTable(years       = 2000:2018,
                                  ages        = 60:100,
                                  sexes       = "Female",
                                  extrapolate = TRUE,
                                  maxExtrap   = 110)

devtools::source_gist("https://gist.github.com/gbrlrgrs/fec0190eb7b5884ca713c61651476b00")
ts <- 20
lw <- 0.5
fnSetGGPlotThemeGR(lw, ts)

tblFem2000_2018 |>
  fnSurvFromLT(CyclesPerYr = 12) |> 
  mutate(Year = paste0(Year_from, "-", sprintf("%02d", Year_to-2000))) |> 
  ggplot() +
  geom_path(aes(x=Age, y=St, colour=Year), linewidth = 1) +
  viridis::scale_colour_viridis(discrete = T, name = "Period") +
  scale_x_continuous(name         = "Age",
                     expand       = c(0,0),
                     breaks       = 4:11*10,
                     minor_breaks = 40:110) +
  scale_y_continuous(name         = "Survival probability",
                     breaks       = 0:5/5,
                     minor_breaks = 0:20/20,
                     expand       = c(0.0001,0)) +
  theme(plot.margin       = margin(1,1,1,1, "cm"),
        axis.text.x       = element_text(size = ts, margin = margin(0.5,0,0,0, "cm")),
        axis.text.y       = element_text(size = ts, margin = margin(0,0.5,0,0, "cm")),
        axis.title.x      = element_text(size = ts, margin = margin(0.5,0,0,0, "cm")),
        axis.title.y      = element_text(size = ts, margin = margin(0,0.5,0,0, "cm")),
        axis.ticks.length = unit(.25, "cm"),
        legend.title      = element_text(size = ts, face = "bold"),
        legend.text       = element_text(size = ts),
        legend.key.width  = unit(x = 1, units = "cm"),
        legend.key.height = unit(x = 0.75, units = "cm"),
        legend.position   = "right",
        legend.margin     = margin(0,0,0,1, "cm"))

fnSaveGG(strName = "60yo_women_surv", cmW = 33.867, cmH = 19.05, typ = "svg")
