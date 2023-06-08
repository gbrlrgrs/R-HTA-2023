library(pacman)
p_load(here,
       tidyverse,
       magrittr)

tblCoD <- nomisr::nomis_get_data(id        = "NM_161_1",
                                 date      = 2013:2021,
                                 geography = 2092957703,
                                 sex       = 1:2,
                                 age       = 1:20,
                                 measure   = 1,
                                 measures  = 20100,
                                 cause_of_death = c(0,9))

tblCoD <- tblCoD |> 
  dplyr::mutate(CoD = ifelse(CAUSE_OF_DEATH==9, "CVD", "All")) |> 
  dplyr::select(Year   = DATE,
                Sex    = GENDER_NAME,
                AgeCat = AGE_NAME,
                AgeC   = AGE_CODE,
                N      = OBS_VALUE,
                CoD) |> 
  tidyr::pivot_wider(values_from = N,
                     names_from  = CoD) |> 
  dplyr::mutate(pCVD   = CVD / All,
                AgeLo  = c(0,1,1:18*5)[AgeC],
                AgeHi  = c(1,1:18*5,999)[AgeC])

tblCoD <- tblCoD |> 
  mutate(AgeCat = factor(AgeCat |> 
                           stringr::str_remove_all("Aged ") |> 
                           stringr::str_replace_all(" to ", "-"),
                         levels = c("under 1", "1-4", paste0(1:17*5, "-", 1:17*5+4), "90 and over"),
                         labels = c("<1", "1-4", paste0(1:17*5, "-", 1:17*5+4), "90+")))

devtools::source_gist("https://gist.github.com/gbrlrgrs/fec0190eb7b5884ca713c61651476b00")
ts = 20
lw = 0.5
fnSetGGPlotThemeGR(lw = lw, ts = ts)

tblCoD |> 
  ggplot2::ggplot(ggplot2::aes(x=Year, y=pCVD, Group=AgeCat)) +
  ggplot2::geom_line(ggplot2::aes(colour=AgeCat)) +
  ggplot2::geom_text(data = tblCoD |> dplyr::group_by(Sex, AgeCat) |> dplyr::slice_tail(n=1),
                     aes(x=Year+0.1, label = AgeCat), hjust=0, size = ts/.pt) +
  ggplot2::facet_grid(cols = ggplot2::vars(Sex)) +
  viridis::scale_colour_viridis(discrete = T, name = "Age group") +
  scale_y_continuous(limits = c(0, 0.35),
                     breaks = 0:10/10,
                     minor_breaks = 0:100/100,
                     expand = c(0, 0),
                     name   = "Proportion of deaths due to CVD") +
  scale_x_continuous(limits = c(2013,2022),
                     breaks = trunc(2014:2022/2)*2,
                     minor_breaks = 2010:2025,
                     expand = expansion(add = c(0.5, 1.5)),
                     name   = "Year") +
  theme(legend.position = "none",
        plot.margin     = margin(1,1,1,1, "cm"))

fnSaveGG(strName = "CVD_deaths", cmW = 33.867, cmH = 19.05, typ = "svg")
