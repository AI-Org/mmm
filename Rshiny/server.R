# Rshiny app author: Grace Gee
# Oringal creation date: March 2014
# Last edited: August 2014
# Data: generated data representing a fashion retailer
# Model: models used in this demo were made up using the generated data; actual
#  models used in the retail lab were Bayesian Hierarchical Models (written in
#  SQL and PL/R by Woo Jung)
# Description: prototype retail app showing sales decomposition reports and
#  what-if scenario analysis using RShiny, PivotalR, MADlib, and GPDB

library(shiny)
library(shinyIncubator) # progress bar
library(ggplot2) # pretty plots
library(reshape) # needed to "melt"/pivot data frame for stacked bar chart 
library(lubridate) # date ops
library(PivotalR)
library(plyr) # progress bar
library(googleVis) # display data frame as interactive table
library(scales) # plat label formatting

# enter database connection info here
db = db.connect(host="localhost", user="gpadmin", password="changeme",
dbname="gpadmin")

# fake generated data
agg.data <- db.data.frame("raw_data", conn.id=db)
transformed.data <- db.data.frame("transformed_data", conn.id=db) 
model.final <- db.data.frame("demo_model", conn.id=db) 

# currently hard code list of features
drivers.planned <- c('num_colors', 'price', 'promo_pct', 'redline_pct')
drivers.planned.textlist <- "num_colors, price, promo_pct, redline_pct"
drivers.planned.names <- c("Week", "Assortment", "Price", "% Items on Promo", "%
Items on Redline")
drivers.raw <- c('num_colors', 'inventory_qty', 'bts_ind',
'holiday_thanksgiving', 'holiday_easter', 'holiday_xmas', 'holiday_pre_xmas',
'price', 'promo_pct', 'redline_pct')
drivers.raw.textlist <- "num_colors, inventory_qty, bts_ind,
holiday_thanksgiving, holiday_easter, holiday_xmas, holiday_pre_xmas, price,
promo_pct, redline_pct"
drivers.transformed <- c("num_colors_log", "inventory_qty_log", "bts_ind",
"holiday_thanksgiving", "holiday_easter", "holiday_xmas", "holiday_pre_xmas",
"price_log", "promo_pct", "redline_pct")
drivers.transformed.textlist <- "num_colors_log, inventory_qty_log, bts_ind,
holiday_thanksgiving, holiday_easter, holiday_xmas, holiday_pre_xmas, price_log,
promo_pct, redline_pct"

# path the save and search for cached plot images
plot_path <- "/home/gpadmin/plots/"
max.files <- 150 # max # of cached plots to keep in plot_path
plot.width <- 900 # width of sales plots -- needed to save png
plot.height <- 450 # height of sales plots -- needed to save png

shinyServer(function(input, output, session) {

  plot.brand <- reactive({
    if(input$plot.brand=="Brand 1")
      'A'
    else
      'B'
  })
  
  plot.gender <- reactive({
    if(input$plot.gender=="Female")
      'F'
    else
      'M'
  })

  # Drop-down selection box for department given brand
  output$ChoosePlotDept <- renderUI({
  if(is.null(input$plot.brand))
      return()
    
    progress <- Progress$new(session)
    progress$set(message = 'Retrieving departments...', value = 0)
    
    dataset <- agg.data
    gender <- plot.gender()
    brand <- plot.brand()

    progress$set(detail = 'reading data...', value = 0.4)

    plot.dept <- c('- All -', lookat(unique(dataset[dataset$brand==brand &
    dataset$gender==gender, ]$dept), -1))
    
    progress$set(val=1)
    progress$close()

    if(length(plot.dept)==0){
      tags$div(style="float:right; padding-right:10px; padding-top:10px;
      color: #990000", "No departments available for selected brand.")
    }else{
      # Drop-down selection box for departments
      selectInput("plot.dept", "Department:", plot.dept)
    }
  })
  
  output$ChoosePlotPeriod <- renderUI({
    if(is.null(input$plot.brand) || is.null(input$plot.dept))
      return()
    
    progress <- Progress$new(session)
    progress$set(message = 'Retrieving available sales period...', value = 0)
    
    dataset <- agg.data
    brand <- plot.brand()
    
    progress$set(detail = 'reading data...', value = 0.5)
      
    # Get the data set with the appropriate brand
    dataset <- dataset[dataset$brand==brand, ]
    
    if(input$plot.dept != '- All -')
      dataset <- dataset[dataset$dept==input$plot.dept, ]
    
    if(input$plot.type != '- All -')
      dataset <- dataset[dataset$type==as.character(input$plot.type), ]
    
    progress$set(val=1)
    progress$close()

    if(dim(dataset)[1]==0){
      tags$div(style="float:right; padding-right:10px; padding-top:10px;
      color: #990000", "No sales data available for selected brand,     
      department, and type.")
    }else{
      week.min <- as.Date(min(lk(unique(dataset$week), -1)))
      week.max <- as.Date(max(lk(unique(dataset$week), -1)))
      
       # Calendar of dates
      dateRangeInput("plot.period", "Sales Period:", 
          start  = week.min, 
          end    = week.max, 
          min    = week.min, 
          max    = week.max+6, 
          format = "mm/dd/yy", 
          separator = " - ") 
    }
  })
  
  # Get subset of data based on user input
  plot.dataset <- reactive({
    brand <- plot.brand()
    dataset <- agg.data
  
    dataset <- dataset[dataset$brand==brand, ]

    if(input$plot.dept != '- All -')
      dataset <- dataset[dataset$dept==input$plot.dept, ]

    if(input$plot.type != '- All -')
      dataset <- dataset[dataset$type==as.character(input$plot.type), ]

    start <- as.Date(floor_date(input$plot.period[1], "week"))
    end <- as.Date(floor_date(input$plot.period[2], "week"))
  
    # FIXME: might be bug with PivotalR 0.1.16.17 -- for now, need to wrap
    # dataset$week in as.character to get subset of data using date ops
    dataset <- dataset[as.character(dataset$week)>=as.character(start) &
      as.character(dataset$week)<=as.character(end)]
 
    dataset
  })
   
  plot.weeks <- reactive({
    start <- as.Date(floor_date(input$plot.period[1], "week"))
    end <- as.Date(floor_date(input$plot.period[2], "week"))
    weeks <- seq(from =  start, to = end, by = 7)
    weeks
  })
  
  decomp.brand <- reactive({
    if(input$historic.decomp.brand=="Brand 1")
      'A'
    else
      'B'
  })
  
  decomp.gender <- reactive({
    if(input$historic.decomp.gender=="Female")
      'F'
    else
      'M'
  })

  # Drop-down selection box for department given brand
  output$ChooseHistoricDecompDept <- renderUI({
    if(is.null(input$historic.decomp.brand))
      return()
    
    progress <- Progress$new(session)
    progress$set(message = 'Retrieving departments...', value = 0)
    
    dataset <- agg.data
    gender <- decomp.gender()
    brand <- decomp.brand()

    progress$set(detail = 'reading data...', value = 0.4)

    decomp_dept <- lookat(unique(dataset[dataset$brand==brand &
    dataset$gender==gender, ]$dept), -1)
    
    progress$set(val=1)
    progress$close()

    if(length(decomp_dept)==0){
      tags$div(style="float:right; padding-right:10px; padding-top:10px;
    color: #990000", "No departments available for selected brand.")
    }else{
      # Drop-down selection box for departments
      selectInput("historic.decomp.dept", "Department:", decomp_dept)
    }
  })
  
  
  output$ChooseHistoricPeriod <- renderUI({
    if(is.null(input$historic.decomp.brand) ||
    is.null(input$historic.decomp.dept)){
      return()
    }
    
    progress <- Progress$new(session)
    progress$set(message = 'Retrieving available sales period...', value = 0)
    
    dataset <- agg.data
    brand <- decomp.brand()
    
    progress$set(detail = 'reading data...', value = 0.5)
      
    # Get the data set with the appropriate brand
    dataset <- dataset[dataset$brand==brand, ]
    
    dataset <- dataset[dataset$dept==input$historic.decomp.dept, ]
    
    dataset <- dataset[dataset$type==as.character(input$historic.decomp.type), ]
    
    progress$set(val=1)
    progress$close()

    if(dim(dataset)[1]==0){
      tags$div(style="float:right; padding-right:10px; padding-top:10px;
      color: #990000", "No sales data available for selected brand, 
      department, and type.")
    }else{
      week.min <- as.Date(min(lk(unique(dataset$week), -1)))
      week.max <- as.Date(max(lk(unique(dataset$week), -1)))
    
       # Calendar of dates
      dateRangeInput("historic.decomp.period", "Sales Period:", 
          start  = week.min, 
          end    = week.max, 
          min    = week.min, 
          max    = week.max+6, 
          format = "mm/dd/yy", 
          separator = " - ") 
    }
  })
  
  whatif.brand <- reactive({
    if(input$whatif.brand=="Brand 1")
      'A'
    else
      'B'
  })
  
  whatif.gender <- reactive({
    if(input$whatif.gender=="Female")
      'F'
    else
      'M'
  })
  
   output$ChooseWhatIfDept <- renderUI({
    if(is.null(input$whatif.brand))
      return()
    
    progress <- Progress$new(session)
    progress$set(message = 'Retrieving departments...', value = 0)
    
    dataset <- agg.data
    gender <- whatif.gender()
    brand <- whatif.brand()

    progress$set(detail = 'reading data...', value = 0.4)

    whatif.dept <- lookat(unique(dataset[dataset$brand==brand &
    dataset$gender==gender, ]$dept), -1)
    
    progress$set(val=1)
    progress$close()

    if(length(whatif.dept)==0){
      tags$div(style="float:right; padding-right:10px; padding-top:10px;
      color: #990000", "No departments available for selected brand.")
    }else{
      # Drop-down selection box for departments
      selectInput("whatif.dept", "Department:", whatif.dept)
    }
  })
  
  
  output$ChooseWhatIfPeriod <- renderUI({
    if(is.null(input$whatif.brand) || is.null(input$whatif.dept))
      return()

    # arbitrary future periods
    week.min <- as.Date('2014-01-01')
    week.max <- as.Date('2014-12-31')

    # Calendar of dates
    dateRangeInput("whatif.period", "Scenario Prediction Period:", 
    start  = week.min, 
    end    = week.max, 
    min    = week.min, 
    max    = week.max+6, 
    format = "mm/dd/yy", 
    separator = " - ") 

  })
  
  reactive.input <- reactiveValues()
  
  # Update what-if scenario planner input table
  planner.data <- reactive({
    dataset <- agg.data
    dataset <- dataset[dataset$dept==input$whatif.dept, ]
    dataset <- dataset[dataset$type==as.character(input$whatif.type), ]
  
    # Set default values = values 1 year ago
    week <- seq(from =  as.Date(floor_date(input$whatif.period[1], "week")), to
      = as.Date(floor_date(input$whatif.period[2], "week")), by = 7)
    num.weeks <- length(week)
    start <- as.Date(floor_date(input$whatif.period[1]-years(1), "week"))
    end <- start+(num.weeks-1)*7
  
    dataset <- dataset[as.character(dataset$week)>=as.character(start) &
      as.character(dataset$week)<=as.character(end)]
  
    # grab user input
    df <- lk(sort(dataset[c("week", drivers.planned)], decreasing=FALSE,
      dataset$week), -1)
    
    d <- data.frame(matrix(0, ncol = length(drivers.planned), nrow =
      length(week)))
    d <- cbind(week, d)
    
    names(d) <-  c("week", drivers.planned)
    d[drivers.planned] <- df[drivers.planned]
    df <- d
    
    reactive.input$data <- df
  })
  
  # Update what-if scenario planner input table by adding # to a whole column
  AddData <- reactive({
    if(input$run.add > 0){
      isolate({
      ndf <- data.frame(input$data)

      idx <- switch(input$update.driver, 
              'Assortment' = 2,
              'Price' = 3, 
              '% Items on Promo' = 4, 
              '% Items on Redline' = 5
              )
      
      ndf[idx] <- ndf[idx] + input$update.num
      reactive.input$data[idx] <- ndf[idx]
    })
  }
  })
  
  SubtractData <- reactive({
    if(input$run.subtract > 0){
      isolate({
      ndf <- data.frame(input$data)

      idx <- switch(input$update.driver, 
              'Assortment' = 2, 
              'Price' = 3, 
              '% Items on Promo' = 4, 
              '% Items on Redline' = 5
              )
      
      ndf[idx] <- ndf[idx] - input$update.num
      reactive.input$data[idx] <- ndf[idx]
    })
  }
  })
  
  # Populate default what-if scenario planner input matrix/table
  output$planner.input <- renderUI({
    if(is.null(input$whatif.brand) || is.null(input$whatif.dept) ||
      is.null(input$whatif.period)){
        return()
    }

    planner.data()
    AddData()
    SubtractData()
  
    df <- reactive.input$data
  
    colnames(df) <- drivers.planned.names
    
    matrixInput(inputId = 'data', label = '', data = df)
  })
  
  # Only output plots the user selected
  output$sales.units.plot <- renderUI({
    if (input$run.plots > 0) {
     isolate({
       plot.list <- c()
       if (input$check.plot.blended.units=='TRUE'){
         plot.list <- c(plot.list, "blended_units.image")
  
         if(input$check.plot.separate=='TRUE')
           plot.list <- c(plot.list, "blended.regular.image",
             "blended.redline.image")
         }
        if (input$check.plot.non.promo.units=='TRUE'){
          plot.list <- c(plot.list, "non.promo.units.image")
          if(input$check.plot.separate=='TRUE')
          plot.list <- c(plot.list, "non.promo.regular.image",
            "non.promo.redline.image")
        }
        if (input$check.plot.promo.units=='TRUE'){
          plot.list <- c(plot.list, "promo.units.image")
          if(input$check.plot.separate=='TRUE'){
            plot.list <- c(plot.list, "promo.regular.image",
              "promo.redline.image")
          }
        }
  
        plot.output.list <- lapply(plot.list, function(i) {
          imageOutput(i, width = "auto", height = "auto")
        })
      
        do.call(tagList, plot.output.list)
      })
    }
  })
  
  output$blended_units.image <- renderImage({
    if (input$run.plots > 0) {
      isolate({
        file.name <- paste(plot_path, plot.brand(), "-", plot.gender(), "-",
          input$plot.dept, "-", input$plot.type, "-", input$plot.period[1], "-",
          input$plot.period[2], "-blended_units", sep="")
    
        if(input$check.plot.price.smooth == TRUE)
           file.name <- paste(file.name, "-smooth.png", sep="")
        else
           file.name <- paste(file.name, ".png", sep="")
  
        file.name <- gsub(" ", "_", file.name)
  
        if(file.exists(file.name)){
          list(src = file.name, contentType = "image/png")
        } 
        else{
          # cache plot for later use; first remove oldest plot if needed
          current.files <- list.files(path = plot_path)
          if(length(current.files) == max.files){
            files.df <- file.info(plot_path)
            oldest.file <- rownames(file.df[with(file.df, order(mtime)),
              ])[1]
          file.remove(oldest.file)
        }
  
        png(file.name, width=plot.width, height=plot.height)
        blended_units.plot()
        dev.off()
        list(src = file.name, contentType = "image/png")
      }
    })
   }
  }, deleteFile = FALSE)
  
  output$blended.regular.image <- renderImage({
    if (input$run.plots > 0) {
      isolate({
        file.name <- paste(plot_path, plot.brand(), "-", plot.gender(), "-",
          input$plot.dept, "-", input$plot.type, "-", input$plot.period[1], "-",
          input$plot.period[2], "-blended_regular", sep="")
       
       if(input$check.plot.price.smooth == TRUE)
         file.name <- paste(file.name, "-smooth.png", sep="")
       else
         file.name <- paste(file.name, ".png", sep="")
  
       file.name <- gsub(" ", "_", file.name)
  
       if(file.exists(file.name)){
         list(src = file.name, contentType = "image/png")
       } 
       else{
         # cache plot for later use; first remove oldest plot if needed
         current.files <- list.files(path = plot_path)
         if(length(current.files) == max.files){
           files.df <- file.info(plot_path)
           oldest.file <- rownames(file.df[with(file.df, order(mtime)),
             ])[1]
           file.remove(oldest.file)
         }
  
        png(file.name, width=plot.width, height=plot.height)
        blended_regular.plot()
        dev.off()
        list(src = file.name, contentType = "image/png")
      }
    })
   }
  }, deleteFile = FALSE)
  
  output$blended.redline.image <- renderImage({
    if (input$run.plots > 0) {
      isolate({
        file.name <- paste(plot_path, plot.brand(), "-", plot.gender(), "-",
          input$plot.dept, "-", input$plot.type, "-", input$plot.period[1], "-",
          input$plot.period[2], "-blended_redline", sep="")
        if(input$check.plot.price.smooth == TRUE)
          file.name <- paste(file.name, "-smooth.png", sep="")
        else
          file.name <- paste(file.name, ".png", sep="")
  
        file.name <- gsub(" ", "_", file.name)
  
        if(file.exists(file.name)){
          list(src = file.name, contentType = "image/png")
        } 
        else{
          # cache plot for later use; first remove oldest plot if needed
          current.files <- list.files(path = plot_path)
          if(length(current.files) == max.files){
            files.df <- file.info(plot_path)
            oldest.file <- rownames(file.df[with(file.df, order(mtime)),
              ])[1]
            file.remove(oldest.file)
        }
  
        png(file.name, width=plot.width, height=plot.height)
        blended_redline.plot()
        dev.off()
        list(src = file.name, contentType = "image/png")
      }
    })
   }
  }, deleteFile = FALSE)
  
  output$non.promo.units.image <- renderImage({
    if (input$run.plots > 0) {
      isolate({
        file.name <- paste(plot_path, plot.brand(), "-", plot.gender(), "-",
          input$plot.dept, "-", input$plot.type, "-", input$plot.period[1], "-",
          input$plot.period[2], "-non_promo.units", sep="")
        if(input$check.plot.price.smooth == TRUE)
          file.name <- paste(file.name, "-smooth.png", sep="")
        else
          file.name <- paste(file.name, ".png", sep="")
      
        file.name <- gsub(" ", "_", file.name)
      
        if(file.exists(file.name)){
          list(src = file.name, contentType = "image/png")
        } 
        else{
          # cache plot for later use; first remove oldest plot if needed
          current.files <- list.files(path = plot_path)
          if(length(current.files) == max.files){
            files.df <- file.info(plot_path)
            oldest.file <- rownames(file.df[with(file.df, order(mtime)),
              ])[1]
            file.remove(oldest.file)
          }
      
          png(file.name, width=plot.width, height=plot.height)
          non.promo.units.plot()
          dev.off()
          list(src = file.name, contentType = "image/png")
      }
    })
   }
  }, deleteFile = FALSE)
  
  output$non.promo.regular.image <- renderImage({
    if (input$run.plots > 0) {
      isolate({
        file.name <- paste(plot_path, plot.brand(), "-", plot.gender(), "-",
          input$plot.dept, "-", input$plot.type, "-", input$plot.period[1], "-",
          input$plot.period[2], "-non_promo_regular", sep="")
        if(input$check.plot.price.smooth == TRUE)
          file.name <- paste(file.name, "-smooth.png", sep="")
        else
          file.name <- paste(file.name, ".png", sep="")
      
        file.name <- gsub(" ", "_", file.name)
      
        if(file.exists(file.name)){
          list(src = file.name, contentType = "image/png")
        } else{
          # cache plot for later use; first remove oldest plot if needed
          current.files <- list.files(path = plot_path)
          if(length(current.files) == max.files){
            files.df <- file.info(plot_path)
            oldest.file <- rownames(file.df[with(file.df, order(mtime)),
              ])[1]
            file.remove(oldest.file)
          }
      
          png(file.name, width=plot.width, height=plot.height)
          non.promo.regular.plot()
          dev.off()
          list(src = file.name, contentType = "image/png")
        }
      })
    }
  }, deleteFile = FALSE)
  
  output$non.promo.redline.image <- renderImage({
    if (input$run.plots > 0) {
      isolate({
        file.name <- paste(plot_path, plot.brand(), "-", plot.gender(), "-",
          input$plot.dept, "-", input$plot.type, "-", input$plot.period[1], "-",
          input$plot.period[2], "-non_promo_redline", sep="")
        if(input$check.plot.price.smooth == TRUE)
          file.name <- paste(file.name, "-smooth.png", sep="")
        else
          file.name <- paste(file.name, ".png", sep="")
      
        file.name <- gsub(" ", "_", file.name)
      
        if(file.exists(file.name)){
          list(src = file.name, contentType = "image/png")
        } else{
          # cache plot for later use; first remove oldest plot if needed
          current.files <- list.files(path = plot_path)
          if(length(current.files) == max.files){
            files.df <- file.info(plot_path)
            oldest.file <- rownames(file.df[with(file.df, order(mtime)),
              ])[1]
            file.remove(oldest.file)
          }
      
          png(file.name, width=plot.width, height=plot.height)
          non_promo.redline.plot()
          dev.off()
          list(src = file.name, contentType = "image/png")
        }
      })
    }
  }, deleteFile = FALSE)
  
  output$promo.units.image <- renderImage({
    if (input$run.plots > 0) {
      isolate({
        file.name <- paste(plot_path, plot.brand(), "-", plot.gender(), "-",
          input$plot.dept, "-", input$plot.type, "-", input$plot.period[1], "-",
          input$plot.period[2], "-promo.units", sep="")
        if(input$check.plot.price.smooth == TRUE)
          file.name <- paste(file.name, "-smooth.png", sep="")
        else
          file.name <- paste(file.name, ".png", sep="")
      
        file.name <- gsub(" ", "_", file.name)
      
        if(file.exists(file.name)){
          list(src = file.name, contentType = "image/png")
        } else{
          # cache plot for later use; first remove oldest plot if needed
          current.files <- list.files(path = plot_path)
          if(length(current.files) == max.files){
            files.df <- file.info(plot_path)
            oldest.file <- rownames(file.df[with(file.df, order(mtime)),
              ])[1]
            file.remove(oldest.file)
          }
      
          png(file.name, width=plot.width, height=plot.height)
          promo.units.plot()
          dev.off()
          list(src = file.name, contentType = "image/png")
        }
      })
    }
  }, deleteFile = FALSE)
  
  output$promo.regular.image <- renderImage({
    if (input$run.plots > 0) {
      isolate({
        file.name <- paste(plot_path, plot.brand(), "-", plot.gender(), "-",
          input$plot.dept, "-", input$plot.type, "-", input$plot.period[1], "-",
          input$plot.period[2], "-promo_regular", sep="")
        if(input$check.plot.price.smooth == TRUE)
          file.name <- paste(file.name, "-smooth.png", sep="")
        else
          file.name <- paste(file.name, ".png", sep="")
      
        file.name <- gsub(" ", "_", file.name)
      
        if(file.exists(file.name)){
          list(src = file.name, contentType = "image/png")
        } else{
          # cache plot for later use; first remove oldest plot if needed
          current.files <- list.files(path = plot_path)
          if(length(current.files) == max.files){
            files.df <- file.info(plot_path)
            oldest.file <- rownames(file.df[with(file.df, order(mtime)),
              ])[1]
            file.remove(oldest.file)
          }
      
          png(file.name, width=plot.width, height=plot.height)
          promo.regular.plot()
          dev.off()
          list(src = file.name, contentType = "image/png")
        }
      })
    }
  }, deleteFile = FALSE)
  
  output$promo.redline.image <- renderImage({
    if (input$run.plots > 0) {
      isolate({
        file.name <- paste(plot_path, plot.brand(), "-", plot.gender(), "-",
          input$plot.dept, "-", input$plot.type, "-", input$plot.period[1], "-",
          input$plot.period[2], "-promo_redline", sep="")
        if(input$check.plot.price.smooth == TRUE)
          file.name <- paste(file.name, "-smooth.png", sep="")
        else
          file.name <- paste(file.name, ".png", sep="")
      
        file.name <- gsub(" ", "_", file.name)
      
        if(file.exists(file.name)){
          list(src = file.name, contentType = "image/png")
        } else{
          # cache plot for later use; first remove oldest plot if needed
          current.files <- list.files(path = plot_path)
          if(length(current.files) == max.files){
            files.df <- file.info(plot_path)
            oldest.file <- rownames(file.df[with(file.df, order(mtime)),
              ])[1]
            file.remove(oldest.file)
          }
      
          png(file.name, width=plot.width, height=plot.height)
          promo.redline.plot()
          dev.off()
          list(src = file.name, contentType = "image/png")
        }
      })
    }
  }, deleteFile = FALSE)

  blended_units.plot <- reactive({
    if (input$run.plots > 0 & input$check.plot.blended.units=='TRUE') {
      isolate({
      # Show progress bar
      progress <- Progress$new(session)
      progress$set(message = 'Blended [all items] plot rendering', value =
        0)
      
      progress$set(detail = 'retrieving data...', value = 0.1)
  
      dataset <- plot.dataset()
      weeks <- plot.weeks()
      num.weeks <- length(weeks)
  
      if(input$check.plot.price.smooth == TRUE){
        total.price.data <-
          dataset[c("blended_price_total_smooth_supsmu", "week")]
      }else{
        total.price.data <- dataset[c("blended_price_total_smooth",
          "week")]
      }
      names(total.price.data)[1] <- 'blended_price_total'
      price.y.min <- lk(min(dataset$blended_price_total_smooth))
      price.y.max <- lk(max(dataset$blended_price_total_smooth))
      unit.step <- 10
      price.step <- 0.10
      
      if(dim(dataset)[1] > 0){
        # extract database table to data frame in order to plot in R 
        progress$set(detail = 'aggregating data...', value = 0.2)
        dataset <- by(dataset[c("blended_units_total")], dataset$week,
          sum)
        total.units <- lk(sort(dataset, decreasing=FALSE, dataset$week))
        total.price.data <-
          by(total.price.data[c("blended_price_total")], 
          total.price.data$week, mean)
        total.price <- lk(sort(total.price.data, decreasing=FALSE,
          total.price.data$week))
      
        progress$set(detail = 'building plots...', value = 0.6)

        plot(total.units$blended_units_total_sum, type="b",
          col="turquoise3", xlab="", ylab="", axes=F, lwd=3)
        axis(1, at=1:num.weeks, labels=weeks)
        pts <- round(seq(from=round_any(
          min(total.units$blended_units_total_sum), unit.step,
          f=floor), to=round_any(max(total.units$blended_units_total_sum), 
          unit.step, f=ceiling), length.out=5))
        pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)

        axis(2, at=pts, labels=sprintf("%s", pts.labels))
        title(xlab="Week", cex.lab = 1.5, font.lab=2,
           col.lab="darkgray")
        title(ylab="Total Blended Units", cex.lab = 1.5, font.lab=2,
          col.lab="darkgray")
        grid()
      
        progress$set(val=0.8)

        par(new=TRUE)
      
        plot(total.price$blended_price_total_avg, type="b",
          col="mediumorchid", xlab="", ylab="", axes=F, lwd=3, 
          ylim=c(price.y.min, price.y.max))
        pts <- seq(from=round_any(price.y.min, price.step, f=floor),
          to=round_any(price.y.max, price.step, f=ceiling), length.out=10)
        pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)

        axis(4, at=pts, labels=sprintf("$%s", pts.labels))
        mtext("Total Blended Price", side=4, cex = 1.5, line=-1, font=2,
          col="darkgray")
    
        legend("topright", inset=c(0.05, -0.1), xpd=TRUE, horiz=TRUE,
          c("All Blended Units", "All Blended Price"), cex=0.9, 
          col=c("turquoise3", "mediumorchid"), lty=c(1, 1))
      
        progress$set(val=1)
      }else{
        progress$set(detail = 'Insufficient data!', value = 1)
        
        progress$close()
        tags$div(style="float:center; color: #990000", "No plot data
          available.")
        return(NULL)
      }
  
      progress$close()
    })
  }else{
    tags$div(style="float:center; color: #990000", "Select data first, then
      click Generate Plots >>>")
    return(NULL)
    }
  })
  
  blended_regular.plot <- reactive({
    if (input$run.plots > 0 & input$check.plot.blended.units=='TRUE' &
      input$check.plot.separate=='TRUE') {
        isolate({
      
          progress <- Progress$new(session)
          progress$set(message = 'Blended [non-redline] plot rendering', 
            value = 0)
          
          progress$set(detail = 'retrieving data...', value = 0.1)
      
          dataset <- plot.dataset()
          weeks <- plot.weeks()
          num.weeks <- length(weeks)
    
          if(input$check.plot.price.smooth == TRUE){
            regular.price.data <-
              dataset[c("blended_price_regular_smooth_supsmu", "week")]
          }else{
            regular.price.data <- dataset[c("blended_price_regular_smooth",
              "week")]
          }
          names(regular.price.data)[1] <- 'blended_price_regular'
          price.y.min <- lk(min(dataset$blended_price_regular_smooth))
          price.y.max <- lk(max(dataset$blended_price_regular_smooth))
          unit.step <- 10
          price.step <- 0.10
          
          if(dim(dataset)[1] > 0){
            progress$set(detail = 'aggregating data...', value = 0.2)
            dataset <- by(dataset[c("blended_units_regular")], dataset$week,
              sum)
            regular.units <- lk(sort(dataset, decreasing=FALSE,
              dataset$week))
            regular.price.data <-  
              by(regular.price.data[c("blended_price_regular")]
              , regular.price.data$week, mean)
            regular.price <- lk(sort(regular.price.data, decreasing=FALSE,
              regular.price.data$week))
        
            progress$set(detail = 'building plots...', value = 0.6)
    
            
            plot(regular.units$blended_units_regular_sum, type="b",
              col="turquoise3", xlab="", ylab="", axes=F, lwd=3)
              axis(1, at=1:num.weeks, labels=weeks)
            pts <- round(seq(from=round_any(
              min(regular.units$blended_units_regular_sum),
              unit.step, f=floor),           
              to=round_any(max(regular.units$blended_units_regular_sum),
              unit.step, f=ceiling), length.out=5))
            pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
            axis(2, at=pts, labels=sprintf("%s", pts.labels))
            title(xlab="Week", cex.lab = 1.5, font.lab=2,
              col.lab="darkgray")
            title(ylab="Non-redline Blended Units", cex.lab = 1.5,
              font.lab=2, col.lab="darkgray")
            grid()
          
            progress$set(val=0.8)
    
            par(new=TRUE)
    
            plot(regular.price$blended_price_regular_avg, type="b",
              col="mediumorchid", xlab="", ylab="", axes=F, lwd=3, 
            ylim=c(price.y.min, price.y.max))
            pts <- seq(from=round_any(price.y.min, price.step, f=floor),
              to=round_any(price.y.max, price.step, f=ceiling), length.out=10)
            pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
            axis(4, at=pts, labels=sprintf("$%s", pts.labels))
            mtext("Non-redline Blended Price", side=4, cex = 1.5, line=-1,
              font=2, col="darkgray")
        
            legend("topright", inset=c(0.05, -0.1), xpd=TRUE, horiz=TRUE,
              c("Non-redline Blended Units", "Non-redline Blended Price"), 
              cex=0.9, col=c("turquoise3", "mediumorchid"), lty=c(1, 1))
    
            progress$set(val=1)
          }else{
            progress$set(detail = 'Insufficient data!', value = 1)
            
            progress$close()
            tags$div(style="float:center; color: #990000", "No plot data
              available.")
            return(NULL)
          }
        progress$close()
      })
    }else{
      tags$div(style="float:center; color: #990000", "Select data first, then
        click Generate Plots >>>")
      return(NULL)
    }
  })

  blended_redline.plot <- reactive({
    if (input$run.plots > 0 & input$check.plot.blended.units=='TRUE' &
      input$check.plot.separate=='TRUE') {
        isolate({
      
          progress <- Progress$new(session)
          progress$set(message = 'Blended [redline] plot rendering', 
            value = 0)
          
          progress$set(detail = 'retrieving data...', value = 0.1)
      
          dataset <- plot.dataset()
          weeks <- plot.weeks()
          num.weeks <- length(weeks)
      
          if(input$check.plot.price.smooth == TRUE){
            redline.price.data <-
              dataset[c("blended_price_redline_smooth_supsmu", "week")]
          }else{
            redline.price.data <- dataset[c("blended_price_redline_smooth",
              "week")]
          }
          names(redline.price.data)[1] <- 'blended_price_redline'
          price.y.min <- lk(min(dataset$blended_price_redline_smooth))
          price.y.max <- lk(max(dataset$blended_price_redline_smooth))
          unit.step <- 10
          price.step <- 2
          
          if(dim(dataset)[1] > 0){
            progress$set(detail = 'aggregating data...', value = 0.2)
            dataset <- by(dataset[c("blended_units_redline")], dataset$week,
              sum)
            redline.units <- lk(sort(dataset, decreasing=FALSE,
              dataset$week))
            redline.price.data <-
              by(redline.price.data[c("blended_price_redline")], 
            redline.price.data$week, mean)
            redline.price <- lk(sort(redline.price.data, decreasing=FALSE,
              redline.price.data$week))
        
            progress$set(detail = 'building plots...', value = 0.6)
            
            plot(redline.units$blended_units_redline_sum, type="b",
              col="turquoise3", xlab="", ylab="", axes=F, lwd=3)
            axis(1, at=1:num.weeks, labels=weeks)
            pts <- round(seq(from=round_any(
              min(redline.units$blended_units_redline_sum),
              unit.step, f=floor), to=round_any(
              max(redline.units$blended_units_redline_sum),
              unit.step, f=ceiling), length.out=5))
            pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
            axis(2, at=pts, labels=sprintf("%s", pts.labels))
            title(xlab="Week", cex.lab = 1.5, font.lab=2,
              col.lab="darkgray")
            title(ylab="Redline Blended Units", cex.lab = 1.5, font.lab=2,
              col.lab="darkgray")
            grid()
          
            progress$set(val=0.8)
    
            par(new=TRUE)
    
            plot(redline.price$blended_price_redline_avg, type="b",
              col="mediumorchid", xlab="", ylab="", axes=F, lwd=3, 
              ylim=c(price.y.min, price.y.max))
            pts <- round(seq(from=round_any(price.y.min, price.step,
              f=floor), to=round_any(price.y.max, price.step, f=ceiling), 
              length.out=10))
            pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
            axis(4, at=pts, labels=sprintf("$%s", pts.labels))
            mtext("Redline Blended Price", side=4, cex = 1.5, line=-1,
              font=2, col="darkgray")
        
            legend("topright", inset=c(0.05, -0.1), xpd=TRUE, horiz=TRUE,
              c("Redline Blended Units", "Redline Blended Price"), cex=0.9,
              col=c("turquoise3", "mediumorchid"), lty=c(1, 1))
    
            progress$set(val=1)
          }else{
            progress$set(detail = 'Insufficient data!', value = 1)
            
            progress$close()
            tags$div(style="float:center; color: #990000", "No plot data
              available.")
            return(NULL)
          }
        progress$close()
      })
    }else{
      tags$div(style="float:center; color: #990000", "Select data first, then
        click Generate Plots >>>")
      return(NULL)
    }
  })

  non.promo.units.plot <- reactive({
    if (input$run.plots > 0 & input$check.plot.non.promo.units=='TRUE') {
      isolate({
      
        progress <- Progress$new(session)
        progress$set(message = 'Non-promo [all items] plot rendering', 
          value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
    
        dataset <- plot.dataset()
        weeks <- plot.weeks()
        num.weeks <- length(weeks)
    
        if(input$check.plot.price.smooth == TRUE){
          total.price.data <-
             dataset[c("non_promo_price_total_smooth_supsmu", "week")]
        }else{
          total.price.data <- dataset[c("non_promo_price_total_smooth",
            "week")]
        }
        names(total.price.data)[1] <- 'non_promo_price_total'
        price.y.min <- lk(min(dataset$non_promo_price_total_smooth))
        price.y.max <- lk(max(dataset$non_promo_price_total_smooth))
        unit.step <- 10
        price.step <- 0.10
        
        if(dim(dataset)[1] > 0){
          progress$set(detail = 'aggregating data...', value = 0.2)
          dataset <- by(dataset[c("non_promo_units_total")], dataset$week,
            sum)
          total.units <- lk(sort(dataset, decreasing=FALSE, dataset$week))
          total.price.data <-
            by(total.price.data[c("non_promo_price_total")], 
            total.price.data$week, mean)
          total.price <- lk(sort(total.price.data, decreasing=FALSE,
            total.price.data$week))
        
          progress$set(detail = 'building plots...', value = 0.6)
          plot(total.units$non_promo_units_total_sum, type="b",
            col="darkorange", xlab="", ylab="", axes=F, lwd=3)
          axis(1, at=1:num.weeks, labels=weeks)
          pts <- round(seq(from=round_any(
             min(total.units$non_promo_units_total_sum), unit.step,
             f=floor), to=round_any(
             max(total.units$non_promo_units_total_sum), unit.step,
            f=ceiling), length.out=5))
          pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
  
          axis(2, at=pts, labels=sprintf("%s", pts.labels))
          title(xlab="Week", cex.lab = 1.5, font.lab=2,
            col.lab="darkgray")
          title(ylab="Total Non-promo Units", cex.lab = 1.5, font.lab=2,
            col.lab="darkgray")
          grid()
        
          progress$set(val=0.8)
  
          par(new=TRUE)
        
          plot(total.price$non_promo_price_total_avg, type="b",
            col="darkolivegreen4", xlab="", ylab="", axes=F, lwd=3, 
            ylim=c(price.y.min, price.y.max))
          pts <- seq(from=round_any(price.y.min, price.step, f=floor),
            to=round_any(price.y.max, price.step, f=ceiling), length.out=10)
          pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
  
          axis(4, at=pts, labels=sprintf("$%s", pts.labels))
          mtext("Total Non-promo Price", side=4, cex = 1.5, line=-1,
            font=2, col="darkgray")
      
          legend("topright", inset=c(0.05, -0.1), xpd=TRUE, horiz=TRUE,
            c("All Non-promo Units", "All Non-promo Price"), cex=0.9, 
            col=c("darkorange", "darkolivegreen4"), lty=c(1, 1))
        
          progress$set(val=1)
        }else{
          progress$set(detail = 'Insufficient data!', value = 1)
          
          progress$close()
          tags$div(style="float:center; color: #990000", "No plot data
            available.")
          return(NULL)
        }
    
        progress$close()
      })
    }else{
      tags$div(style="float:center; color: #990000", "Select data first, then
        click Generate Plots >>>")
      return(NULL)
    }
  })
  
  non.promo.regular.plot <- reactive({
    if (input$run.plots > 0 & input$check.plot.non.promo.units=='TRUE' &
      input$check.plot.separate=='TRUE') {
      isolate({
      
        progress <- Progress$new(session)
        progress$set(message = 'Non-promo [non-redline] plot rendering',
          value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
    
        dataset <- plot.dataset()
        weeks <- plot.weeks()
        num.weeks <- length(weeks)
  
        if(input$check.plot.price.smooth == TRUE){
          regular.price.data <-
            dataset[c("non_promo_price_regular_smooth_supsmu", "week")]
        }else{
          regular.price.data <-
            dataset[c("non_promo_price_regular_smooth", "week")]
        }
        names(regular.price.data)[1] <- 'non_promo_price_regular'
        price.y.min <- lk(min(dataset$non_promo_price_regular_smooth))
        price.y.max <- lk(max(dataset$non_promo_price_regular_smooth))
        unit.step <- 10
        price.step <- 0.10
        
        if(dim(dataset)[1] > 0){
          progress$set(detail = 'aggregating data...', value = 0.2)
          dataset <- by(dataset[c("non_promo_units_regular")],
            dataset$week, sum)
          regular.units <- lk(sort(dataset, decreasing=FALSE,
            dataset$week))
          regular.price.data <-
            by(regular.price.data[c("non_promo_price_regular")], 
            regular.price.data$week, mean)
          regular.price <- lk(sort(regular.price.data, decreasing=FALSE,
            regular.price.data$week))
      
          progress$set(detail = 'building plots...', value = 0.6)
          plot(regular.units$non_promo_units_regular_sum, type="b",
            col="darkorange", xlab="", ylab="", axes=F, lwd=3)
          axis(1, at=1:num.weeks, labels=weeks)
          pts <- round(seq(from=round_any(
            min(regular.units$non_promo_units_regular_sum),
            unit.step, f=floor), to=round_any(
            max(regular.units$non_promo_units_regular_sum), unit.step,
            f=ceiling), length.out=5))
          pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
          axis(2, at=pts, labels=sprintf("%s", pts.labels))
          title(xlab="Week", cex.lab = 1.5, font.lab=2,
            col.lab="darkgray")
          title(ylab="Non-redline Non-promo Units", cex.lab = 1.5,
            font.lab=2, col.lab="darkgray")
          grid()
        
          progress$set(val=0.8)
  
          par(new=TRUE)
  
          plot(regular.price$non_promo_price_regular_avg, type="b",
            col="darkolivegreen4", xlab="", ylab="", axes=F, lwd=3, 
            ylim=c(price.y.min, price.y.max))
          pts <- seq(from=round_any(price.y.min, price.step, f=floor),
            to=round_any(price.y.max, price.step, f=ceiling), length.out=10)
          pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
          axis(4, at=pts, labels=sprintf("$%s", pts.labels))
          mtext("Non-redline Non-promo Price", side=4, cex = 1.5, line=-1,
            font=2, col="darkgray")
      
          legend("topright", inset=c(0.05, -0.1), xpd=TRUE, horiz=TRUE,
            c("Non-redline Non-promo Units", "Non-redline Non-promo Price"), 
            cex=0.9, col=c("darkorange", "darkolivegreen4"), lty=c(1, 1))
        
          progress$set(val=1)
        }else{
          progress$set(detail = 'Insufficient data!', value = 1)
          
          progress$close()
          tags$div(style="float:center; color: #990000", "No plot data
            available.")
          return(NULL)
        }
        progress$close()
      })
    }else{
      tags$div(style="float:center; color: #990000", "Select data first, then
        click Generate Plots >>>")
      return(NULL)
    }
  })
   
  non_promo.redline.plot <- reactive({
    if (input$run.plots > 0 & input$check.plot.non.promo.units=='TRUE' &
      input$check.plot.separate=='TRUE') {
      isolate({
        
        progress <- Progress$new(session)
        progress$set(message = 'Non-promo [redline] plot rendering', 
          value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
    
        dataset <- plot.dataset()
        weeks <- plot.weeks()
        num.weeks <- length(weeks)
    
        if(input$check.plot.price.smooth == TRUE){
          redline.price.data <-
            dataset[c("non_promo_price_redline_smooth_supsmu", "week")]
        }else{
          redline.price.data <-
            dataset[c("non_promo_price_redline_smooth", "week")]
        }
        names(redline.price.data)[1] <- 'non_promo_price_redline'
        price.y.min <- lk(min(dataset$non_promo_price_redline_smooth))
        price.y.max <- lk(max(dataset$non_promo_price_redline_smooth))
        unit.step <- 10
        price.step <- 2
        
        if(dim(dataset)[1] > 0){
          progress$set(detail = 'aggregating data...', value = 0.2)
          dataset <- by(dataset[c("non_promo_units_redline")],
            dataset$week, sum)
          redline.units <- lk(sort(dataset, decreasing=FALSE,
            dataset$week))
          redline.price.data <-
            by(redline.price.data[c("non_promo_price_redline")], 
            redline.price.data$week, mean)
          redline.price <- lk(sort(redline.price.data, decreasing=FALSE,
            redline.price.data$week))
      
          progress$set(detail = 'building plots...', value = 0.6)
          plot(redline.units$non_promo_units_redline_sum, type="b",
            col="darkorange", xlab="", ylab="", axes=F, lwd=3)
          axis(1, at=1:num.weeks, labels=weeks)
          pts <- round(seq(from=round_any(
            min(redline.units$non_promo_units_redline_sum),
            unit.step, f=floor), to=round_any(
            max(redline.units$non_promo_units_redline_sum), unit.step,
            f=ceiling), length.out=5))
          pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
          axis(2, at=pts, labels=sprintf("%s", pts.labels))
          title(xlab="Week", cex.lab = 1.5, font.lab=2,
            col.lab="darkgray")
          title(ylab="Redline Non-promo Units", cex.lab = 1.5, font.lab=2,
            col.lab="darkgray")
          grid()
        
          progress$set(val=0.8)
  
          par(new=TRUE)
  
          plot(redline.price$non_promo_price_redline_avg, type="b",
            col="darkolivegreen4", xlab="", ylab="", axes=F, lwd=3, 
            ylim=c(price.y.min, price.y.max))
          pts <- round(seq(from=round_any(price.y.min, price.step,
            f=floor), to=round_any(price.y.max, price.step, f=ceiling), 
            length.out=10))
          pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
          axis(4, at=pts, labels=sprintf("$%s", pts.labels))
          mtext("Redline Non-promo Price", side=4, cex = 1.5, line=-1,
            font=2, col="darkgray")
      
          legend("topright", inset=c(0.05, -0.1), xpd=TRUE, horiz=TRUE,
            c("Redline Non-promo Units", "Redline Non-promo Price"), cex=0.9,
            col=c("darkorange", "darkolivegreen4"), lty=c(1, 1))
        
          progress$set(val=1)
        }else{
          progress$set(detail = 'Insufficient data!', value = 1)
          
          progress$close()
          tags$div(style="float:center; color: #990000", "No plot data
            available.")
          return(NULL)
        }
        progress$close()
      })
    }else{
      tags$div(style="float:center; color: #990000", "Select data first, then
        click Generate Plots >>>")
      return(NULL)
    }
  })
  

  promo.units.plot <- reactive({
    if (input$run.plots > 0 & input$check.plot.promo.units=='TRUE') {
      isolate({
      
      progress <- Progress$new(session)
      progress$set(message = 'Promo [all items] plot rendering', 
        value = 0)
      
      progress$set(detail = 'retrieving data...', value = 0.1)
  
      dataset <- plot.dataset()
      weeks <- plot.weeks()
      num.weeks <- length(weeks)
  
      if(input$check.plot.price.smooth == TRUE){
        total.price.data <- dataset[c("promo_price_total_smooth_supsmu",
          "week")]
      }else{
        total.price.data <- dataset[c("promo_price_total_smooth",
          "week")]
      }
      names(total.price.data)[1] <- 'promo_price_total'
      price.y.min <- lk(min(dataset$promo_price_total_smooth))
      price.y.max <- lk(max(dataset$promo_price_total_smooth))
      unit.step <- 10
      price.step <- 0.10
      
      if(dim(dataset)[1] > 0){
        progress$set(detail = 'aggregating data...', value = 0.2)
        dataset <- by(dataset[c("promo_units_total")], dataset$week,
          sum)
        total.units <- lk(sort(dataset, decreasing=FALSE, dataset$week))
        total.price.data <- by(total.price.data[c("promo_price_total")],
          total.price.data$week, mean)
        total.price <- lk(sort(total.price.data, decreasing=FALSE,
          total.price.data$week))
      
        progress$set(detail = 'building plots...', value = 0.6)
        plot(total.units$promo_units_total_sum, type="b", col="gold",
          xlab="", ylab="", axes=F, lwd=3)
        axis(1, at=1:num.weeks, labels=weeks)
        pts <- round(seq(from=round_any(
          min(total.units$promo_units_total_sum), unit.step,
          f=floor), to=round_any(
          max(total.units$promo_units_total_sum), unit.step,
          f=ceiling), length.out=5))
        pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
        
        axis(2, at=pts, labels=sprintf("%s", pts.labels))
        title(xlab="Week", cex.lab = 1.5, font.lab=2,
          col.lab="darkgray")
        title(ylab="Total Promo Units", cex.lab = 1.5, font.lab=2,
          col.lab="darkgray")
        grid()
      
        progress$set(val=0.8)

        par(new=TRUE)
      
        plot(total.price$promo_price_total_avg, type="b", col="hotpink",
          xlab="", ylab="", axes=F, lwd=3, ylim=c(price.y.min, price.y.max))
        pts <- seq(from=round_any(price.y.min, price.step, f=floor),
          to=round_any(price.y.max, price.step, f=ceiling), length.out=10)
        pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
        
        axis(4, at=pts, labels=sprintf("$%s", pts.labels))
        mtext("Total Promo Price", side=4, cex = 1.5, line=-1, font=2,
          col="darkgray")
    
        legend("topright", inset=c(0.05, -0.1), xpd=TRUE, horiz=TRUE,
          c("All Promo Units", "All Promo Price"), cex=0.9, col=c("gold", 
          "hotpink"), lty=c(1, 1))
      
        progress$set(val=1)
      }else{
        progress$set(detail = 'Insufficient data!', value = 1)
        
        progress$close()
        tags$div(style="float:center; color: #990000", "No plot data
          available.")
        return(NULL)
      }
  
      progress$close()
    })
  }else{
    tags$div(style="float:center; color: #990000", "Select data first, then
      click Generate Plots >>>")
    return(NULL)
  }
  })
  
  promo.regular.plot <- reactive({
    if (input$run.plots > 0 & input$check.plot.promo.units=='TRUE' &
      input$check.plot.separate=='TRUE') {
      isolate({
      
        progress <- Progress$new(session)
        progress$set(message = 'Promo [non-redline] plot rendering', 
          value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
    
        dataset <- plot.dataset()
        weeks <- plot.weeks()
        num.weeks <- length(weeks)
  
        if(input$check.plot.price.smooth == TRUE){
          regular.price.data <-
            dataset[c("promo_price_regular_smooth_supsmu", "week")]
        }else{
          regular.price.data <- dataset[c("promo_price_regular_smooth",
            "week")]
        }
        names(regular.price.data)[1] <- 'promo_price_regular'
        price.y.min <- lk(min(dataset$promo_price_regular_smooth))
        price.y.max <- lk(max(dataset$promo_price_regular_smooth))
        unit.step <- 10
        price.step <- 0.10
        
        if(dim(dataset)[1] > 0){
          progress$set(detail = 'aggregating data...', value = 0.2)
          dataset <- by(dataset[c("promo_units_regular")], dataset$week,
            sum)
          regular.units <- lk(sort(dataset, decreasing=FALSE,
            dataset$week))
          regular.price.data <-
            by(regular.price.data[c("promo_price_regular")], 
            regular.price.data$week, mean)
          regular.price <- lk(sort(regular.price.data, decreasing=FALSE,
            regular.price.data$week))
      
          progress$set(detail = 'building plots...', value = 0.6)
          plot(regular.units$promo_units_regular_sum, type="b",
            col="gold", xlab="", ylab="", axes=F, lwd=3)
          axis(1, at=1:num.weeks, labels=weeks)
          pts <- round(seq(from=round_any(
            min(regular.units$promo_units_regular_sum), unit.step,
            f=floor), to=round_any
            (max(regular.units$promo_units_regular_sum), unit.step,
            f=ceiling), length.out=5))
          pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
          axis(2, at=pts, labels=sprintf("%s", pts.labels))
          title(xlab="Week", cex.lab = 1.5, font.lab=2,
            col.lab="darkgray")
          title(ylab="Non-redline Promo Units", cex.lab = 1.5, font.lab=2,
            col.lab="darkgray")
          grid()
        
          progress$set(val=0.8)
  
          par(new=TRUE)
  
          plot(regular.price$promo_price_regular_avg, type="b",
            col="hotpink", xlab="", ylab="", axes=F, lwd=3, ylim=c(price.y.min,
            price.y.max))
          pts <- seq(from=round_any(price.y.min, price.step, f=floor),
            to=round_any(price.y.max, price.step, f=ceiling), length.out=10)
          pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
          axis(4, at=pts, labels=sprintf("$%s", pts.labels))
          mtext("Non-redline Promo Price", side=4, cex = 1.5, line=-1,
            font=2, col="darkgray")
      
          legend("topright", inset=c(0.05, -0.1), xpd=TRUE, horiz=TRUE,
            c("Non-redline Promo Units", "Non-redline Promo Price"), cex=0.9,         
            col=c("gold", "hotpink"), lty=c(1, 1))
        
          progress$set(val=1)
        }else{
          progress$set(detail = 'Insufficient data!', value = 1)
          
          progress$close()
          tags$div(style="float:center; color: #990000", "No plot data
            available.")
          return(NULL)
        }
        progress$close()
      })
    }else{
      tags$div(style="float:center; color: #990000", "Select data first, then
        click Generate Plots >>>")
      return(NULL)
    }
  })
 
  promo.redline.plot <- reactive({
    if (input$run.plots > 0 & input$check.plot.promo.units=='TRUE' &
      input$check.plot.separate=='TRUE') {
      isolate({
      
        progress <- Progress$new(session)
        progress$set(message = 'Promo [redline] plot rendering', value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
    
        dataset <- plot.dataset()
        weeks <- plot.weeks()
        num.weeks <- length(weeks)
    
        if(input$check.plot.price.smooth == TRUE){
          redline.price.data <-
            dataset[c("promo_price_redline_smooth_supsmu", "week")]
        }else{
          redline.price.data <- dataset[c("promo_price_redline_smooth",
            "week")]
        }
        names(redline.price.data)[1] <- 'promo_price_redline'
        price.y.min <- lk(min(dataset$promo_price_redline_smooth))
        price.y.max <- lk(max(dataset$promo_price_redline_smooth))
        unit.step <- 10
        price.step <- 2
        
        if(dim(dataset)[1] > 0){
          progress$set(detail = 'aggregating data...', value = 0.2)
          dataset <- by(dataset[c("promo_units_redline")], dataset$week,
            sum)
          redline.units <- lk(sort(dataset, decreasing=FALSE,
            dataset$week))
          redline.price.data <-
            by(redline.price.data[c("promo_price_redline")], 
            redline.price.data$week, mean)
          redline.price <- lk(sort(redline.price.data, decreasing=FALSE,
            redline.price.data$week))
      
          progress$set(detail = 'building plots...', value = 0.6)
          plot(redline.units$promo_units_redline_sum, type="b",
            col="gold", xlab="", ylab="", axes=F, lwd=3)
          axis(1, at=1:num.weeks, labels=weeks)
          pts <- round(seq(from=round_any(
            min(redline.units$promo_units_redline_sum), unit.step,
            f=floor), to=round_any(
            max(redline.units$promo_units_redline_sum), unit.step,
            f=ceiling), length.out=5))
          pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
          axis(2, at=pts, labels=sprintf("%s", pts.labels))
          title(xlab="Week", cex.lab = 1.5, font.lab=2,
            col.lab="darkgray")
          title(ylab="Redline Promo Units", cex.lab = 1.5, font.lab=2,
            col.lab="darkgray")
          grid()
        
          progress$set(val=0.8)
  
          par(new=TRUE)
  
          plot(redline.price$promo_price_redline_avg, type="b",
            col="hotpink", xlab="", ylab="", axes=F, lwd=3, ylim=c(price.y.min,
            price.y.max))
          pts <- round(seq(from=round_any(price.y.min, price.step,
            f=floor), to=round_any(price.y.max, price.step, f=ceiling), 
            length.out=10))
          pts.labels <- format(round(pts, 2), big.mark=", ", scientific=F)
          axis(4, at=pts, labels=sprintf("$%s", pts.labels))
          mtext("Redline Promo Price", side=4, cex = 1.5, line=-1, font=2,
            col="darkgray")
      
          legend("topright", inset=c(0.05, -0.1), xpd=TRUE, horiz=TRUE,
            c("Redline Promo Units", "Redline Promo Price"), cex=0.9, 
            col=c("gold", "hotpink"), lty=c(1, 1))
        
          progress$set(val=1)
        }else{
          progress$set(detail = 'Insufficient data!', value = 1)
          
          progress$close()
          tags$div(style="float:center; color: #990000", "No plot data
            available.")
          return(NULL)
        }
        progress$close()
      })
    }else{
      tags$div(style="float:center; color: #990000", "Select data first, then
        click Generate Plots >>>")
      return(NULL)
    }
  })
  
  # even though have one plot, do this so the page doesn't unnecessarily
  # dedicate empty space for a plot if the user doesn't actually want to plot it
  output$stacked.units.plot <- renderUI({
    if (input$run.plots > 0) {
      isolate({
        # search to see if there is already an existing plot
        file.name <- paste(plot_path, plot.brand(), "-", plot.gender(), "-",
          input$plot.dept, "-", input$plot.type, "-", input$plot.period[1], "-",
          input$plot.period[2], "-stacked.units.png", sep="")
        file.name <- gsub(" ", "_", file.name)
    
      # if plot exists, display the png
      if(file.exists(file.name)){
        imageOutput("stacked.image")
      } else{
        # create plot on the fly
        plotOutput("stacked.plot")
      }
    })
   }
  })  
  
  output$stacked.image <- renderImage({
    if (input$run.plots > 0) {
      isolate({
        file.name <- paste(plot_path, plot.brand(), "-", plot.gender(), "-",
           input$plot.dept, "-", input$plot.type, "-", input$plot.period[1], 
           "-", input$plot.period[2], "-stacked.units.png", sep="")
        file.name <- gsub(" ", "_", file.name)
        list(src = file.name, contentType = "image/png")
      })
    }
  }, deleteFile = FALSE)
      
  output$stacked.plot <- renderPlot({
    if (input$run.plots > 0) {
      isolate({
        if(input$check.plot.stacked.units=='TRUE'){
          
        progress <- Progress$new(session)
        progress$set(message = 'Stacked sales plot rendering', value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
        
        dataset <- plot.dataset()
  
        units <- lk(by(dataset[c("non_promo_units_regular",
          "non_promo_units_redline", "promo_units_regular", 
          "promo_units_redline")], dataset$week, sum), -1)
        
        names(units) <- c("week", "Non-redline, non-promo units", "Redline,
          non-promo units", "Non-redline, promo units", "Redline, promo units")
        
        progress$set(detail = 'generating plot...', value = 0.4)
      
        if(dim(dataset)[1] > 0){
          # Using ggplot to plot
          units.m <- melt(units, id=c("week")) # week | col_name | col_value
          a <- ggplot(units.m, aes(x = week, y = value, 
            fill = variable)) + theme(title = element_text("Sales by
            Markdown and Promo")) + labs(x = NULL, y = "Unit Sales", 
            fill = NULL)
          b <- a + geom_bar(stat = "identity", position = "stack") +
            scale_y_continuous(labels = comma)
          print(b)
          
          new_file <- paste(plot_path, plot.brand(), "-", plot.gender(),
            "-", input$plot.dept, "-", input$plot.type, "-", 
            input$plot.period[1], "-", input$plot.period[2], 
            "-stacked.units.png", sep="")
          new_file <- gsub(" ", "_", new_file)
          ggsave(new_file)
          
        }else{
          progress$set(detail = 'Insufficient data!', value = 1)
          
          progress$close()
          tags$div(style="float:center; color: #990000", "No plot data
            available.")
          return(NULL)
        }
        
        progress$set(val=1)
        progress$close()
      }
      })
    }
  })
  
  table.options <- reactive({
    list(
      page=ifelse(input$pageable==TRUE, 'enable', 'disable'), 
      pageSize=input$pagesize, 
      height=750, width=1100
    )
  })
  list(titleTextStyle="{color:red, fontName:Courier, fontSize:16}")
  
  output$raw.table <- renderGvis({
    if (input$run.plots > 0) {
      isolate({
    
        progress <- Progress$new(session)
        progress$set(message = 'Generating raw data table...', value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
      
        dataset <- plot.dataset()
      
        # FIXME: Set arbitraty max rows to display
        if(input$pagesize > 200){
          tags$div(style="float:center; color: #990000", "Too much data for R
            to display! Please choose a smaller page size.")
          return(NULL)
        }
    
        progress$set(detail = 'creating table...', value = 0.8)
        
        cols <- c("week", "dept", "type", "blended_units_total",
          "blended_units_regular", "blended_units_redline", 
          "non_promo_units_total",  "non_promo_units_regular", 
          "non_promo_units_redline", "promo_units_total",
          "promo_units_regular", "promo_units_redline")
    
          if(input$check.plot.price.smooth == TRUE){
            cols <- c(cols, "blended_price_regular_smooth_supsmu",
              "blended_price_redline_smooth_supsmu",    
              "non_promo_price_regular_smooth_supsmu",
              "non_promo_price_redline_smooth_supsmu",  
              "promo_price_regular_smooth_supsmu",
              "promo_price_redline_smooth_supsmu")
          }
          else{
            cols <- c(cols, "blended_price_regular_smooth",
              "blended_price_redline_smooth",  "non_promo_price_regular_smooth",
              "non_promo_price_redline_smooth",  "promo_price_regular_smooth",
              "promo_price_redline_smooth")
          }
    
        subdata <- dataset[cols]
        names(subdata) <- c("Week", "Department", "type",
          "Total_Blended_Unit_Sales", "Non_redline_Blended_Unit_Sales",
          "Redline_Blended_Unit_Sales", "Total_Non_promo_Unit_Sales",
          "Non_redline_Non_promo_Unit_Sales", "Redline_Non_promo_Unit_Sales",
          "Total_Promo_Unit_Sales", "Non_redline_Promo_Unit_Sales",
          "Redline_Promo_Unit_Sales", "Non_redline_Blended_Price",
          "Redline_Blended_Price", "Non_redline_Non_promo_Price",
          "Redline_Non_promo_Price", "Non_redline_Promo_Price", 
          "Redline_Promo_Price")
    
        subtable <- gvisTable(lk(subdata, -1), options=table.options())   
        
        progress$set(val=1)
        progress$close()
        
        return(subtable)
        })
      }   
  })
  
  
  
  
  
  
  
  ########################## Simulated Sales Decomp ######################
  
  
  output$predictions.data <- renderGvis({
    if (input$run.whatif.scenario > 0) {
      isolate({
      
        predict()

        progress <- Progress$new(session)
        progress$set(message = 'Generating raw data table...', value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
      
        subdata <- db.q("select dept, type, week, y_hat as
          sales_units_predictions, y as last_years_sales from sim_predictions",
          conn.id=db, nrows=-1)
    
        subtable <- gvisTable(subdata, options=table.options())   
        
        progress$set(val=1)
        progress$close()
        
        return(subtable)
        })
      }   
  })
  
  whatif.group <- reactive({
    group <- "" # 'group by' query to split decomp plots
    if(input$whatif.decomp.group == "Quarter")
      group <- ", quarter"
    else if(input$whatif.decomp.group == "Month")
      group <- ", fiscal_month"
    
    return(group)
  })
  
  output$predictions.decomp.pct <- renderGvis({
    if (input$run.whatif.scenario > 0) {
      isolate({
        
        progress <- Progress$new(session)
        progress$set(message = 'Generating raw data table...', value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
        
        group_query <- whatif.group() # 'group by' query to split decomp plots
    
        subdata <- db.q("select dept as department, type as store
          , base_sales_pct
          , decomp_bts_pct + decomp_thanksgiving_pct + decomp_easter_pct + 
            decomp_xmas_pct + decomp_pre_xmas_pct as decomp_seasonality_pct
          , decomp_promo_pct as decomp_promo_pct
          , decomp_redline_pct as decomp_redline_pct"
          , group_query
          , ", year from sim_decomps_pct", conn.id=db, nrows=-1)

        subtable <- gvisTable(subdata, options=table.options())   
        
        progress$set(val=1)
        progress$close()
        
        return(subtable)
      })
    }   
  })
  
  # Generate simulated sales given user input
  predict <- reactive({
    
    progress <- Progress$new(session)
    progress$set(message = 'Generating sales predictions', value = 0)
    
    progress$set(detail = 'retrieving model...', value = 0.05)

    brand <- whatif.brand()
    model <- model.final # database table with 3 cols: dept, type, coef 
    model <- model[model$dept==input$whatif.dept, ] 
    
    model <- model[model$type==as.character(input$whatif.type), ]
    if(dim(model)[1] == 0){
      plot(NULL)
      title(main="No demand model available for selected department and
        type!", col.main="red", font.main=2)
      return()
    }

    dataset <- agg.data
    dataset <- dataset[dataset$dept==input$whatif.dept, ]
    dataset <- dataset[dataset$type==as.character(input$whatif.type), ]

    week <- seq(from =  as.Date(floor_date(input$whatif.period[1], "week")),
      to = as.Date(floor_date(input$whatif.period[2], "week")), by = 7)
    num.weeks <- length(week)
    start <- as.Date(floor_date(input$whatif.period[1]-years(1), "week"))
    end <- start+(num.weeks-1)*7
    
    dataset <- dataset[as.character(dataset$week)>=as.character(start) &
      as.character(dataset$week)<=as.character(end)]
    
    past_sales <- lk(sort(dataset$blended_units_total, decreasing=FALSE,
      dataset$week), -1)
    
    db.q("drop table if exists x_new; drop table if exists x_transformed;
      drop table if exists sim_predictions", conn.id=db, nrows=-1)
    
    levers <- data.frame(input$data)
    names(levers) <- c("week", drivers.planned)
    
    # Use last year's default values, but replace them with planner levers
    dataset <- lk(sort(dataset, decreasing=FALSE, dataset$week), -1)

    dataset$week <- week
    
    dataset[drivers.planned] <- levers[drivers.planned]

    if(dim(dataset)[1] == 0){
      print("No data entered for selected product and time period!")
      plot(NULL)
      title(main="No data entered for selected product and time period!",
        col.main="red", font.main=2)
      return()
    }
    
    progress$set(detail = 'transforming data...', value = 0.2)
    
    x <- as.db.data.frame(dataset, verbose=FALSE, conn.id=db, is.temp=TRUE)

    # Get user input and transform variables
    # group as many queries together to lower latency
    query <- paste("
      create temp table x_new_dup as select dept, type, week, quarter,
        fiscal_month, blended_units_total, ", drivers.raw.textlist, " from ",
        content(x), "; 
        
      create temp table x_new as select distinct * from x_new_dup; 
      drop table x_new_dup; 
      
      create temp table x_transformed as select d.dept, d.type,
        d.week, d.quarter, d.fiscal_month
        , ln(blended_units_total+1) as blended_units_total_log
        , ln(num_colors+1) as num_colors_log
        , ln(inventory_qty+1) as inventory_qty_log
        , bts_ind
        , holiday_thanksgiving
        , holiday_easter
        , holiday_xmas
        , holiday_pre_xmas
        , ln(price+1) as price_log
        , promo_pct
        , redline_pct 
      from x_new as d;")
        
    db.q(query, conn.id=db, nrows=-1)
        
    dataset <- db.data.frame("x_transformed", conn.id=db)
    dataset <- dataset[c("dept", "type", "week", "quarter", "fiscal_month",
      "blended_units_total_log",  drivers.transformed)]
      
    # Ignore weeks where there is a NULL value for an attribute
    for (i in 1:dim(dataset)[2]) 
      dataset <- dataset[!is.na(dataset[i]), ] 
      
    progress$set(detail = 'generating predictions...', value = 0.6)
    
    # Forecast sales
    query <- paste("
      create temp table sim_predictions as select d.dept, d.type, d.week,
        d.quarter, d.fiscal_month
        , exp(blended_units_total_log)-1 as y
        , round(exp(madlib.array_dot(array[1, ", drivers.transformed.textlist, 
            "]::double precision[], m.coef))-1) as y_hat 
      from (", content(dataset), ") as d 
      join (", content(model), ") as m 
      on d.dept=m.dept and d.type=m.type 
      order by d.week; 
      
      drop table if exists sim_past_predictions;")
    db.q(query, conn.id=db, nrows=-1)
    
    old_data <- transformed.data
    old_data <- old_data[old_data$dept==input$whatif.dept, ]
    old_data <- old_data[old_data$type==as.character(input$whatif.type), ]
    old_data <- old_data[as.character(old_data)$week>=as.character(start) &
      as.character(old_data)$week<=as.character(end)]

    # Get last year's simulated sales
    query <- paste("
    
      create temp table sim_past_predictions as select d.dept,
        d.type, week
        , exp(blended_units_total_log)-1 as y
        , round(exp(madlib.array_dot(array[1, ", drivers.transformed.textlist,    
            "]::double precision[], m.coef))-1) as y_hat 
      from (", content(old_data), ") as d 
      join (", content(model), ") as m 
      on d.dept=m.dept and d.type=m.type 
      order by d.week;")
      
    db.q(query, conn.id=db, nrows=-1)

    progress$set(val=1)
    progress$close()
    
    past_sales
  })
  
  output$predictions <- renderUI({ 
    if (input$run.whatif.scenario > 0) {
      isolate({
      
        past_sales <- predict()
        
        delta <- db.q("
          select round(((sum(n.y_hat) - sum(o.y_hat)) / 
            sum(o.y_hat)::float)::numeric, 4) 
          from sim_predictions as n, sim_past_predictions as o 
          where n.dept=o.dept and n.type=o.type and 
          n.week=date_trunc('week', o.week::date+365)::date-1",
          conn.id=db, nrows=-1)

        pred <- db.q("select round(sum(y_hat)) from sim_predictions",
          conn.id=db, nrows=-1)

        past_sim <- db.q("select round(sum(y_hat)) from sim_past_predictions",
          conn.id=db, nrows=-1)

        past_actual <- db.q("select round(sum(y)) from sim_predictions",
          conn.id=db, nrows=-1)

        pred_plus_err <- pred+(past_actual-past_sim)

        if(delta < 0){
          change <- "decrease"
          color <- "#FF3333"
        }
        else{
          change <- "increase"
          color <- "#00CC99"
        }
        
        start <- as.character(as.Date(floor_date(input$whatif.period[1],
          "week")))
          
        end <- as.character(as.Date(floor_date(input$whatif.period[2], "week")))
        HTML(paste('<p><p><font color=#00CC99 size=4 face=\"Lucida Sans
          Unicode\">Simulated sales for [', start, '] - [', end, 
          ']:</font><p><font color=', color, ' size=7 face=\"Lucida Sans 
          Unicode\"><center><strong>', delta*100, '%<br></font><font 
          color=#00CC99 size=5 face=\"Lucida Sans Unicode\">', change, ' in 
          simulated sales from last year</strong></font><p><p><font 
          color=#33CCFF size=2 face=\"Lucida Sans Unicode\">New simulated sales: 
          ', pred,'<br>Last year simulated sales: ', past_sim, '<br>Last year 
          actual sales: ', past_actual, '<br>Simulated sales + model err: ', 
          pred_plus_err, '</center></font>'))
      })
    }
  }) 
    
  # Get decomp plot for simulated sales
  output$predictions.decomp <- renderPlot({
     
    if (input$run.whatif.scenario > 0) {
      isolate({
          
        predict()
  
        progress <- Progress$new(session)
        progress$set(message = 'Generating simulated sales decomp report',
          value = 0)
      
        progress$set(detail = 'retrieving data...', value = 0.05)
    
        brand <- decomp.brand()
  
        is.matrix(input$data) # gimmick to grab user data
  
        dataset <- db.data.frame("x_transformed", conn.id=db)
        group_query <- whatif.group()
      
        start <- as.Date(floor_date(input$whatif.period[1], "week"))
        end <- as.Date(floor_date(input$whatif.period[2], "week"))
  
        if(dim(dataset)[1] == 0){
          print("No data available for selected product and time period!")
          plot(NULL)
          title(main="No data available for selected product and time
             period!", col.main="red", font.main=2)
          return()
        }
    
        model <- model.final
        # database table with 3 cols: dept, type, coef (array)
        model <- model[model$dept==input$whatif.dept, ] 
        model <- model[model$type==as.character(input$whatif.type), ]
        if(dim(model)[1] == 0){
          plot(NULL)
          title(main="No demand model available for selected department
            and type!", col.main="red", font.main=2)
          return()
        }
      
        dataset <- dataset[c("dept", "type", "week", "quarter",
           "fiscal_month", "blended_units_total_log", drivers.transformed)]
  
        for (i in 1:dim(dataset)[2]) 
          dataset <- dataset[!is.na(dataset[i]),] 
      
        progress$set(detail = 'calculating decomp...', value = 0.3)
      
        ############## Decomp calc done in GPDB ###############
        db.q("
          drop table if exists sim_residuals; 
          drop table if exists sim_raw_decomps; 
          drop table if exists sim_decomp_sums; 
          drop table if exists sim_decomps_corrected; 
          drop table if exists sim_decomps_pct; 
          create temp table sim_residuals as 
            select *, y-y_hat as err from sim_predictions;
        ", conn.id=db, nrows=-1)
        
        # FIXME: Change below lines if drivers change! Hardcoded for now...
        # Include effect of base price, assortment, and inventory drivers in
  		#   base_sales (along with intercept)
        # For decomps, reference values must be the transformed values
        query <- paste("
        
          create temp table sim_raw_decomps as 
            select *
        	, decomp_bts + decomp_thanksgiving + decomp_easter + decomp_xmas 
        	  + decomp_pre_xmas + decomp_promo + decomp_redline as decomp_sum
        	, y_hat - (base_sales + decomp_bts + decomp_thanksgiving 
        	  + decomp_easter + decomp_xmas + decomp_pre_xmas + decomp_promo 
        	  + decomp_redline) as decomp_err 
            from (
              select d.dept, d.type, d.week
              , extract(year from d.week::date) as year
              , d.quarter, d.fiscal_month, y_hat
        	  , exp(m.coef[1] + m.coef[9] * price_log + m.coef[2] 
        	    * num_colors_log + m.coef[3] * inventory_qty_log) as base_sales
              , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
                  inventory_qty_log, 0, holiday_thanksgiving, holiday_easter, 
                  holiday_xmas, holiday_pre_xmas, price_log, promo_pct,  
                  redline_pct]::double precision[], m.coef))  - 1) as decomp_bts
              , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
                  inventory_qty_log, bts_ind, 0, holiday_easter, holiday_xmas, 
                  holiday_pre_xmas, price_log, promo_pct, redline_pct]::double 
                  precision[], m.coef))  - 1) as decomp_thanksgiving
              , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
                  inventory_qty_log, bts_ind, holiday_thanksgiving, 0, 
                  holiday_xmas, holiday_pre_xmas, price_log, promo_pct, 
                  redline_pct]::double precision[], m.coef))  - 1) as 
                  decomp_easter
              , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
                  inventory_qty_log, bts_ind, holiday_thanksgiving, 
                  holiday_easter, 0, holiday_pre_xmas, price_log, promo_pct, 
                  redline_pct]::double precision[], m.coef))  - 1) as 
                  decomp_xmas
              , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
                  inventory_qty_log, bts_ind, holiday_thanksgiving, 
                  holiday_easter, holiday_xmas, 0, price_log, promo_pct,  
                  redline_pct]::double precision[], m.coef))  - 1) as
                  decomp_pre_xmas
              , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
                  inventory_qty_log, bts_ind, holiday_thanksgiving,         
                  holiday_easter, holiday_xmas, holiday_pre_xmas, price_log, 0, 
                  redline_pct]::double precision[], m.coef))  - 1) as 
                  decomp_promo
              , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
                  inventory_qty_log, bts_ind, holiday_thanksgiving, 
                  holiday_easter, holiday_xmas, holiday_pre_xmas, price_log, 
                  promo_pct, 0]::double precision[], m.coef))  - 1) as 
                  decomp_redline  
              from (", content(dataset), ") as d 
              left join (", content(model), ") as m 
              on d.dept=m.dept and d.type=m.type 
              left join sim_predictions as p 
              on d.dept=p.dept and d.type=p.type and d.week=p.week
            ) as foo 
            order by week;", "
            
          create temp table sim_decomp_sums as select dept, type
            , sum(y_hat) as y_hat_sum
            , sum(base_sales) as base_sales_sum
            , sum(decomp_bts) as decomp_bts_sum
            , sum(decomp_thanksgiving) as decomp_thanksgiving_sum
            , sum(decomp_easter) as decomp_easter_sum
            , sum(decomp_xmas) as decomp_xmas_sum
            , sum(decomp_pre_xmas) as decomp_pre_xmas_sum
            , sum(decomp_promo) as decomp_promo_sum
            , sum(decomp_redline) as decomp_redline_sum "
            , group_query, 
            ", year 
          from sim_raw_decomps 
          group by dept, type, year", group_query, ";", "
          
          create temp table sim_decomps_corrected as select dept, type
            , round(base_sales_sum) as base_sales
            , round(decomp_bts_sum+decomp_err*decomp_bts_sum/decomp_sum::float)
                as decomp_bts
            , round(decomp_thanksgiving_sum + decomp_err * 
                decomp_thanksgiving_sum / decomp_sum::float) as 
                decomp_thanksgiving
            , round(decomp_easter_sum + decomp_err * decomp_easter_sum / 
                decomp_sum::float) as decomp_easter
            , round(decomp_xmas_sum + decomp_err * decomp_xmas_sum / 
                decomp_sum::float) as decomp_xmas
            , round(decomp_pre_xmas_sum + decomp_err * decomp_pre_xmas_sum / 
                decomp_sum::float) as decomp_pre_xmas
            , round(decomp_promo_sum + decomp_err * decomp_promo_sum / 
                decomp_sum::float) as decomp_promo
            , round(decomp_redline_sum + decomp_err * decomp_redline_sum / 
                decomp_sum::float) as decomp_redline"
            , group_query
            , ", year 
          from (
            select *
            , decomp_bts_sum + decomp_thanksgiving_sum + decomp_easter_sum + 
                decomp_xmas_sum + decomp_pre_xmas_sum + decomp_promo_sum + 
                decomp_redline_sum as decomp_sum
            , y_hat_sum - (base_sales_sum + decomp_bts_sum + decomp_thanksgiving_sum + 
                decomp_easter_sum + decomp_xmas_sum + decomp_pre_xmas_sum +  
                decomp_promo_sum + decomp_redline_sum) as decomp_err 
            from sim_decomp_sums
          ) as foo;", "
          
          create temp table sim_decomps_pct as select dept, type
            , round(greatest(base_sales/decomp_sum::float, 0)::numeric, 2) as 
                base_sales_pct
            , round(greatest(decomp_bts/decomp_sum::float, 0)::numeric, 2) as
                decomp_bts_pct
            , round(greatest(decomp_thanksgiving/decomp_sum::float, 0)::numeric, 
                2) as decomp_thanksgiving_pct
            , round(greatest(decomp_easter/decomp_sum::float, 0)::numeric, 2) as
                decomp_easter_pct
            , round(greatest(decomp_xmas/decomp_sum::float, 0)::numeric, 2) as
                decomp_xmas_pct
            , round(greatest(decomp_pre_xmas/decomp_sum::float, 0)::numeric, 2) 
                as decomp_pre_xmas_pct
            , round(greatest(decomp_promo/decomp_sum::float, 0)::numeric, 2) as
                decomp_promo_pct
            , round(greatest(decomp_redline/decomp_sum::float, 0)::numeric, 2) 
                as decomp_redline_pct"
            , group_query
            , ", year 
          from (
            select *
            ,  base_sales+decomp_bts + decomp_thanksgiving + decomp_easter + 
                 decomp_xmas + decomp_pre_xmas + decomp_promo + decomp_redline 
                 as decomp_sum 
            from sim_decomps_corrected
          ) as foo;")
        db.q(query, conn.id=db, nrows=-1)
        
        # reduce number of drivers for demo
        decomp_data <- db.q("
          select dept, type
          , base_sales
          , decomp_bts + decomp_thanksgiving + decomp_easter + decomp_xmas 
              + decomp_pre_xmas as decomp_seasonality
          , decomp_promo
          , decomp_redline"
          , group_query
          , ", year 
          from sim_decomps_corrected;
        ", conn.id=db, nrows=-1)
        
        names(decomp_data)[3] <- "Base Sales (including Price, Inventory &
          Assortment)"
        names(decomp_data)[4] <- "Seasonality"
        names(decomp_data)[5] <- "% Items on Promo"
        names(decomp_data)[6] <- "% Items on Redline"
      
        progress$set(detail = 'rendering graph...', value = 0.8)
  
        if(input$whatif.decomp.group == "Quarter"){
          decomp_data.m <- melt(decomp_data[, 3:8], id=c("year",
            "quarter"))
        
          # reorder plot x-axis labels
          # (to avoid default alphabetical order, which is incorrect)
          decomp_data.m$labels <- paste(decomp_data.m$year,
            decomp_data.m$quarter, sep = "-")
          decomp_data.m$labels <- factor(decomp_data.m$labels,
            decomp_data.m[order(decomp_data.m$year, decomp_data.m$quarter), 
            ]$labels)
        
          # re-sort data frame 
          decomp_data.m <- decomp_data.m[order(decomp_data.m$year,
            decomp_data.m$quarter), ]
          # remove zero %'s
          decomp_data.m <- decomp_data.m[decomp_data.m$value > 2, ]
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pos
            = cumsum(value) - 0.5*value)
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pct
            = round(value/sum(value)*100, 2))
        
          a <- ggplot(decomp_data.m, aes(x = labels, y = value, 
          fill = variable)) + theme(title = element_text("Simulated Sales
            Decomposition")) +
            labs(x = paste("Sales Period Week of [",
            floor_date(input$whatif.period[1], "week"), "] - [",
            floor_date(input$whatif.period[2], "week"), "]"), y = "Simulated 
            Sales Units", fill = NULL)
        }
        else if(input$whatif.decomp.group == "Month"){
          decomp_data.m <- melt(decomp_data[, 3:8], id=c("year",
            "fiscal_month"))
          # reorder factor levels for month so x-axis is in right order
          decomp_data.m$fiscal_month <- factor(decomp_data.m$fiscal_month,
            levels=c("JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", 
              "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER"))
          levels(decomp_data.m$fiscal_month) <- c("01", "02", "03", "04",
            "05", "06", "07", "08", "09", "10", "11", "12")
        
          # reorder plot x-axis labels 
          # (to avoid default alphabetical order, which is incorrect)
          decomp_data.m$labels <- paste(decomp_data.m$year,
            decomp_data.m$fiscal_month, sep = "-")
          decomp_data.m$labels <- factor(decomp_data.m$labels,
            decomp_data.m[order(decomp_data.m$year, decomp_data.m$fiscal_month), 
            ]$labels)
        
          # re-sort data frame 
          decomp_data.m <- decomp_data.m[order(decomp_data.m$year,
            decomp_data.m$fiscal_month), ]
          # remove zero %'s
          decomp_data.m <- decomp_data.m[decomp_data.m$value > 2, ]
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pos
            = cumsum(value) - 0.5*value)
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pct
            = round(value/sum(value)*100, 2))
        
          a <- ggplot(decomp_data.m, aes(x = labels, y = value, 
            fill = variable)) + theme(title = element_text("Simulated Sales
            Decomposition")) +
            labs(x = paste("Sales Period Week of [",
            floor_date(input$whatif.period[1], "week"), "] - [",
            floor_date(input$whatif.period[2], "week"), "]"), y = "Simulated 
            Sales Units", fill = NULL)
        }
        else{
          decomp_data <- db.q("
            select dept, type
            , sum(base_sales) as base_sales
            , sum(decomp_bts + decomp_thanksgiving + decomp_easter + decomp_xmas 
                + decomp_pre_xmas) as decomp_seasonality
            , sum(decomp_promo) as decomp_promo
            , sum(decomp_redline) as decomp_redline
            from sim_decomps_corrected
            group by dept, type;
          ", conn.id=db, nrows=-1)
          
          names(decomp_data)[3] <- "Base Sales (including Price, Inventory &
          Assortment)"
          names(decomp_data)[4] <- "Seasonality"
          names(decomp_data)[5] <- "% Items on Promo"
          names(decomp_data)[6] <- "% Items on Redline"
          
          decomp_data.m <- melt(decomp_data, id=c("dept", "type"))
  
          decomp_data.m$labels <- paste(decomp_data.m$dept,
            decomp_data.m$type, sep = "-")
          # remove zero %'s
          decomp_data.m <- decomp_data.m[decomp_data.m$value > 2, ]
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pos
            = cumsum(value) - 0.5*value)
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pct
            = round(value/sum(value)*100, 2))
        
          a <- ggplot(decomp_data.m, aes(x = labels, y = value, 
            fill = variable)) + theme(title = element_text("Simulated Sales
            Decomposition")) +
            labs(x = paste("Sales Period Week of [",
            floor_date(input$whatif.period[1], "week"), "] - [",
            floor_date(input$whatif.period[2], "week"), "]"), y = "Simulated 
            Sales Units", fill = NULL)
        }
      
        b <- a + geom_bar(stat = "identity", position = "stack") +
          scale_y_continuous(labels = comma)  + geom_text(aes(label = paste(pct, 
          "%", sep=""), y= pos), size=4)
        print(b)
  
        progress$set(val=1)
        progress$close()
      })
    }
  })
  
  ########################## Historic Sales Decomp ######################
      
  # Insert an existing graph or create a new one
  output$find.historic.decomp.plot <- renderUI({
    
    if (input$run.historic.decomp > 0) {
      isolate({
        
        # search to see if there is already an existing plot
        file.name <- paste(plot_path, decomp.brand(), "-", decomp.gender(), "-",
          input$historic.decomp.dept, "-", input$historic.decomp.type, "-",
          input$historic.decomp.period[1], "-", input$historic.decomp.period[2], 
            "-", input$historic.decomp.group, ".png", sep="")
        file.name <- gsub(" ", "_", file.name)
        
        # if plot exists, display the png
        if(file.exists(file.name)){
          imageOutput("historic.decomp.image")
        } else{
          # create plot on the fly (may need to change text in progress bars)
          plotOutput("historic.decomp.plot")
        }
        
      })
    }
  })
  
  output$historic.decomp.image <- renderImage({
    if (input$run.historic.decomp > 0) {
      isolate({
        file.name <- paste(plot_path, decomp.brand(), "-", decomp.gender(), "-",
          input$historic.decomp.dept, "-", input$historic.decomp.type, "-",
          input$historic.decomp.period[1], "-", input$historic.decomp.period[2], 
          "-", input$historic.decomp.group, ".png", sep="")
        file.name <- gsub(" ", "_", file.name)
        list(src = file.name, contentType = "image/png")
      })
    }
  }, deleteFile = FALSE)
  
  historic.group <- reactive({
    group <- "" # 'group by' query to split decomp plots
    if(input$historic.decomp.group == "Quarter")
      group <- ", quarter"
    else if(input$historic.decomp.group == "Month")
      group <- ", fiscal_month"
  })
    
  decomp <- reactive({
    
    progress <- Progress$new(session)
    progress$set(message = 'Generating sales decomp report', value = 0)
    
    progress$set(detail = 'retrieving data...', value = 0.05)
  
    brand <- decomp.brand()
    group_query <- historic.group()
          
    dataset <- transformed.data
    dataset <- dataset[dataset$dept==input$historic.decomp.dept, ]
    dataset <- dataset[dataset$type == as.character(input$historic.decomp.type)
      , ]

    start <- as.Date(floor_date(input$historic.decomp.period[1], "week"))
    end <- as.Date(floor_date(input$historic.decomp.period[2], "week"))
    dataset <- dataset[as.character(dataset$week)>=as.character(start) &
      as.character(dataset$week)<=as.character(end)]
    num.weeks <- as.numeric((end-start)/7)+1
        
    if(dim(dataset)[1] == 0){
      print("No data available for selected product and time period!")
      plot(NULL)
      title(main="No data available for selected product and time
        period!", col.main="red", font.main=2)
      return()
    }

    model <- model.final
    model <- model[model$dept==input$historic.decomp.dept, ] 
    model <- model[model$type==as.character(input$historic.decomp.type), ]
    if(dim(model)[1] == 0){
      plot(NULL)
      title(main="No demand model available for selected department and
        type!", col.main="red", font.main=2)
      return()
    }
  
    dataset <- dataset[c("dept", "type", "week", "quarter", "fiscal_month",
      "blended_units_total_log", drivers.transformed)]

    for (i in 1:dim(dataset)[2]) 
      dataset <- dataset[!is.na(dataset[i]),] 
    
    progress$set(detail = 'calculating decomp...', value = 0.3)
    
    ################## Decomp calc done in GPDB #################
    db.q("
      drop table if exists predictions; 
      drop table if exists residuals;
      drop table if exists raw_decomps; 
      drop table if exists decomp_sums; 
      drop table if exists decomps_corrected; 
      drop table if exists decomps_pct;
    ", conn.id=db, nrows=-1)

    query <- paste("
      create temp table predictions as select d.dept, d.type, week
      , exp(blended_units_total_log)-1 as y
      , exp(madlib.array_dot(array[1, ", drivers.transformed.textlist, 
          "]::double precision[], m.coef))-1 as y_hat 
      from (", content(dataset), ") as d 
      join (", content(model), ") as m 
      on d.dept=m.dept and d.type=m.type order by d.week; 
        
      create temp table residuals as select *
      , y-y_hat as err 
      from predictions; ", "
      
      create temp table raw_decomps as select *
      , decomp_bts + decomp_thanksgiving + decomp_easter + decomp_xmas +
          decomp_pre_xmas + decomp_promo + decomp_redline as decomp_sum
      , y_hat - (base_sales + decomp_bts + decomp_thanksgiving + decomp_easter
          + decomp_xmas + decomp_pre_xmas + decomp_promo + decomp_redline) as 
          decomp_err 
      from (
        select d.dept, d.type, d.week, y_hat
        , exp(m.coef[1] + m.coef[9] * price_log + m.coef[2] * num_colors_log 
            + m.coef[3] * inventory_qty_log) as base_sales
        , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
            inventory_qty_log, 0, holiday_thanksgiving, holiday_easter, 
            holiday_xmas, holiday_pre_xmas, price_log, promo_pct, 
            redline_pct]::double precision[], m.coef))  - 1) as decomp_bts
        , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
            inventory_qty_log, bts_ind, 0, holiday_easter, holiday_xmas, 
            holiday_pre_xmas, price_log, promo_pct, redline_pct]::double 
            precision[], m.coef))  - 1) as decomp_thanksgiving
        , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
            inventory_qty_log, bts_ind, holiday_thanksgiving, 0, holiday_xmas,
            holiday_pre_xmas, price_log, promo_pct, redline_pct]::double 
            precision[], m.coef))  - 1) as decomp_easter
        , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
            inventory_qty_log, bts_ind, holiday_thanksgiving, holiday_easter, 0,
            holiday_pre_xmas, price_log, promo_pct, redline_pct]::double 
            precision[], m.coef))  - 1) as decomp_xmas
        , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
            inventory_qty_log, bts_ind, holiday_thanksgiving, holiday_easter, 
            holiday_xmas, 0, price_log, promo_pct, redline_pct]::double 
            precision[], m.coef))  - 1) as decomp_pre_xmas
        , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
            inventory_qty_log, bts_ind, holiday_thanksgiving, holiday_easter, 
            holiday_xmas, holiday_pre_xmas, price_log, 0, redline_pct]::double 
            precision[], m.coef))  - 1) as decomp_promo
        , y_hat - (exp(madlib.array_dot(array[1, num_colors_log,
            inventory_qty_log, bts_ind, holiday_thanksgiving, holiday_easter, 
            holiday_xmas, holiday_pre_xmas, price_log, promo_pct, 0]::double 
            precision[], m.coef))  - 1) as decomp_redline"
        , group_query
        , ", extract(year from d.week) as year 
        from (", content(dataset), ") as d 
        left join (", content(model), ") as m 
        on d.dept=m.dept and d.type=m.type 
        left join predictions as p 
        on d.dept=p.dept and d.type=p.type and d.week=p.week
      ) as foo 
      order by week;", "
      
      create temp table decomp_sums as select dept, type
      , sum(y_hat) as y_hat_sum
      , sum(base_sales) as base_sales_sum
      , sum(decomp_bts) as decomp_bts_sum
      , sum(decomp_thanksgiving) as decomp_thanksgiving_sum
      , sum(decomp_easter) as decomp_easter_sum
      , sum(decomp_xmas) as decomp_xmas_sum
      , sum(decomp_pre_xmas) as decomp_pre_xmas_sum
      , sum(decomp_promo) as decomp_promo_sum
      , sum(decomp_redline) as decomp_redline_sum"
      , group_query
      , ", year 
      from raw_decomps 
      group by dept, type, year", group_query, ";", "
      
      create temp table decomps_corrected as select dept, type
      , round(base_sales_sum) as base_sales
      , round(decomp_bts_sum+decomp_err*decomp_bts_sum/decomp_sum::float) as
          decomp_bts
      , round(decomp_thanksgiving_sum + decomp_err * decomp_thanksgiving_sum / 
          decomp_sum::float) as decomp_thanksgiving
      , round(decomp_easter_sum + decomp_err * decomp_easter_sum / 
          decomp_sum::float) as decomp_easter
      , round(decomp_xmas_sum + decomp_err * decomp_xmas_sum / 
          decomp_sum::float) as decomp_xmas
      , round(decomp_pre_xmas_sum + decomp_err * decomp_pre_xmas_sum / 
          decomp_sum::float) as decomp_pre_xmas
      , round(decomp_promo_sum + decomp_err * decomp_promo_sum / 
          decomp_sum::float) as decomp_promo
      , round(decomp_redline_sum + decomp_err * decomp_redline_sum / 
          decomp_sum::float) as decomp_redline"
      , group_query
      , ", year 
      from (
        select *
        , decomp_bts_sum + decomp_thanksgiving_sum + decomp_easter_sum 
            + decomp_xmas_sum + decomp_pre_xmas_sum + decomp_promo_sum +
            decomp_redline_sum as decomp_sum
        , y_hat_sum - (base_sales_sum + decomp_bts_sum + decomp_thanksgiving_sum + 
            decomp_easter_sum + decomp_xmas_sum + decomp_pre_xmas_sum +  
            decomp_promo_sum + decomp_redline_sum) as decomp_err 
        from decomp_sums
      ) as foo;", "
      
      create temp table decomps_pct as select dept, type
      , round(greatest(base_sales / decomp_sum::float, 0)::numeric, 2) as 
          base_sales_pct
      , round(greatest(decomp_bts/decomp_sum::float, 0)::numeric, 2) as
          decomp_bts_pct
      , round(greatest(decomp_thanksgiving/decomp_sum::float, 0)::numeric, 2)
          as decomp_thanksgiving_pct
      , round(greatest(decomp_easter/decomp_sum::float, 0)::numeric, 2) as
          decomp_easter_pct
      , round(greatest(decomp_xmas/decomp_sum::float, 0)::numeric, 2) as
          decomp_xmas_pct
      , round(greatest(decomp_pre_xmas/decomp_sum::float, 0)::numeric, 2) as
          decomp_pre_xmas_pct
      , round(greatest(decomp_promo/decomp_sum::float, 0)::numeric, 2) as
          decomp_promo_pct
      , round(greatest(decomp_redline/decomp_sum::float, 0)::numeric, 2) as
          decomp_redline_pct"
      , group_query 
      , ", year 
      from (
        select *
        , base_sales+decomp_bts + decomp_thanksgiving + decomp_easter + 
            decomp_xmas + decomp_pre_xmas + decomp_promo + decomp_redline as
            decomp_sum 
        from decomps_corrected
      ) as foo;")
      
      # Create temp predictions table with 3 cols: week, y, y_hat
      db.q(query, conn.id=db, nrows=-1) 
      
      progress$set(val=1)
      progress$close()
    })
    
  output$historic.decomp.data <- renderGvis({
    if (input$run.historic.decomp > 0) {
      isolate({
      
        decomp()
    
        progress <- Progress$new(session)
        progress$set(message = 'Generating raw data table...', value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
        
        group_query <- historic.group()
        
        subdata <- db.q("
          select dept as department, type as store, base_sales
            , decomp_bts + decomp_thanksgiving + decomp_easter + decomp_xmas + 
                decomp_pre_xmas as decomp_seasonality
            , decomp_promo as decomp_promo
            , decomp_redline as decomp_redline"
            , group_query
            , ", year 
          from decomps_corrected
        ", conn.id=db, nrows=-1)
    
    
        subtable <- gvisTable(subdata, options=table.options())   
        
        progress$set(val=1)
        progress$close()
        
        return(subtable)
        })
      }   
  })
  
  output$historic.decomp.pct <- renderGvis({
    if (input$run.historic.decomp > 0) {
      isolate({
      
        decomp()
        
        progress <- Progress$new(session)
        progress$set(message = 'Generating % data table...', value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
        
        group_query <- historic.group()
      
        subdata <- db.q("
          select dept as department, type as store, base_sales_pct
            , decomp_bts_pct + decomp_thanksgiving_pct + decomp_easter_pct + 
                decomp_xmas_pct + decomp_pre_xmas_pct as decomp_seasonality_pct
            , decomp_promo_pct as decomp_promo_pct
            , decomp_redline_pct as decomp_redline_pct"
            , group_query
            , ", year 
          from decomps_pct
        ", conn.id=db, nrows=-1)
    
        subtable <- gvisTable(subdata, options=table.options())   
        
        progress$set(val=1)
        progress$close()
        
        return(subtable)
        })
      }   
  })

  output$historic.decomp.plot <- renderPlot({
    if (input$run.historic.decomp > 0) {
      isolate({
        decomp()
        
        progress <- Progress$new(session)
        progress$set(message = 'Generating sales decomp visual...', value = 0)
        
        progress$set(detail = 'retrieving data...', value = 0.1)
    
        group_query <- historic.group()
    
        decomp_data <- db.q("
          select dept, type, base_sales
            , decomp_bts + decomp_thanksgiving + decomp_easter + decomp_xmas + 
                decomp_pre_xmas as decomp_seasonality
            , decomp_promo 
            , decomp_redline"
            , group_query
            , ", year 
          from decomps_corrected;
        ", conn.id=db, nrows=-1)
        
        names(decomp_data)[3] <- "Base Sales (including Price, Inventory &
          Assortment)"
        names(decomp_data)[4] <- "Seasonality"
        names(decomp_data)[5] <- "% Items on Promo"
        names(decomp_data)[6] <- "% Items on Redline"
        
        progress$set(detail = 'rendering graph...', value = 0.8)
    
        if(input$historic.decomp.group == "Quarter"){
          decomp_data.m <- melt(decomp_data[, 3:8], id=c("year", "quarter"))
          
          decomp_data.m$labels <- paste(decomp_data.m$year,
            decomp_data.m$quarter, sep = "-")
          decomp_data.m$labels <- factor(decomp_data.m$labels,
            decomp_data.m[order(decomp_data.m$year, decomp_data.m$quarter), 
            ]$labels)
          
          # re-sort data frame 
          decomp_data.m <- decomp_data.m[order(decomp_data.m$year,
            decomp_data.m$quarter), ]
          # remove zero %'s
          decomp_data.m <- decomp_data.m[decomp_data.m$value > 2, ]
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pos =
            cumsum(value) - 0.5*value)
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pct =
            round(value/sum(value)*100, 2))
          
          a <- ggplot(decomp_data.m, aes(x = labels, y = value, 
          fill = variable)) + theme(title = element_text("Historic Sales
            Decomposition")) +
            labs(x = paste("Sales Period Week of [",
            floor_date(input$historic.decomp.period[1], "week"), "] - [",
            floor_date(input$historic.decomp.period[2], "week"), "]"), y = 
            "Historic Sales Units", fill = NULL)
        }
        else if(input$historic.decomp.group == "Month"){
          decomp_data.m <- melt(decomp_data[, 3:8], id=c("year",
            "fiscal_month"))
          
          # reorder factor levels for month so x-axis is in right order
          decomp_data.m$fiscal_month <- factor(decomp_data.m$fiscal_month,
            levels=c("JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", 
            "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER"), 
            ordered=TRUE)
          levels(decomp_data.m$fiscal_month) <- c("01", "02", "03", "04",
            "05", "06", "07", "08", "09", "10", "11", "12")

          decomp_data.m$labels <- paste(decomp_data.m$year,
            decomp_data.m$fiscal_month, sep = "-")
          decomp_data.m$labels <- factor(decomp_data.m$labels,
            decomp_data.m[order(decomp_data.m$year, decomp_data.m$fiscal_month), 
            ]$labels)
          
          # re-sort data frame 
          decomp_data.m <- decomp_data.m[order(decomp_data.m$year,
            decomp_data.m$fiscal_month), ]
          # remove zero %'s
          decomp_data.m <- decomp_data.m[decomp_data.m$value > 2, ]
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pos =
            cumsum(value) - 0.5*value)
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pct =
            round(value/sum(value)*100, 2))
                 
          a <- ggplot(decomp_data.m, aes(x = labels, y = value, 
            fill = variable)) + theme(title = element_text("Historic Sales
            Decomposition")) +
            labs(x = paste("Sales Period Week of [",
            floor_date(input$historic.decomp.period[1], "week"), "] - [",
            floor_date(input$historic.decomp.period[2], "week"), "]"), y = 
            "Historic Sales Units", fill = NULL)
        }
        else{
          decomp_data <- db.q("
            select dept, type
            , sum(base_sales) as base_sales
            , sum(decomp_bts + decomp_thanksgiving + decomp_easter + decomp_xmas + 
                decomp_pre_xmas) as decomp_seasonality
            , sum(decomp_promo) as decomp_promo 
            , sum(decomp_redline) as decomp_redline 
            from decomps_corrected
            group by dept, type;
          ", conn.id=db, nrows=-1)
          names(decomp_data)[3] <- "Base Sales (including Price, Inventory &
          Assortment)"
          names(decomp_data)[4] <- "Seasonality"
          names(decomp_data)[5] <- "% Items on Promo"
          names(decomp_data)[6] <- "% Items on Redline"
          
          decomp_data.m <- melt(decomp_data, id=c("dept", "type"))
          
          decomp_data.m$labels <- paste(decomp_data.m$dept,
            decomp_data.m$type, sep = "-")
          # remove zero %'s
          decomp_data.m <- decomp_data.m[decomp_data.m$value > 2, ]
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pos =
            cumsum(value) - 0.5*value)
          decomp_data.m <- ddply(decomp_data.m, .(labels), transform, pct =
            round(value/sum(value)*100, 2))
          
          a <- ggplot(decomp_data.m, aes(x = labels, y = value, 
            fill = variable)) + theme(title = element_text("Historic Sales
            Decomposition")) +
            labs(x = paste("Sales Period Week of [",
            floor_date(input$historic.decomp.period[1], "week"), "] - [",
            floor_date(input$historic.decomp.period[2], "week"), "]"), y = 
            "Historic Sales Units", fill = NULL)
        }
        
        b <- a + geom_bar(stat = "identity", position = "stack") +
          scale_y_continuous(labels = comma)  + geom_text(aes(label = paste(pct, 
          "%", sep=""), y= pos), size=4)
        print(b)
        
        # cache plot for later use
        current.files <- list.files(path = plot_path)
        if(length(current.files) == max.files){
          files.df <- file.info(plot_path)
          oldest.file <- rownames(files.df[with(files.df, order(mtime)), ])[1]
          file.remove(oldest.file)
        }
        
        new_file <- paste(plot_path, decomp.brand(), "-", decomp.gender(), "-",
          input$historic.decomp.dept, "-", input$historic.decomp.type, "-",
          input$historic.decomp.period[1], "-", input$historic.decomp.period[2], 
          "-", input$historic.decomp.group, ".png", sep="")
        new_file <- gsub(" ", "_", new_file)
        ggsave(new_file)
        
        progress$set(val=1)
        progress$close()
      })
    }
  })


})