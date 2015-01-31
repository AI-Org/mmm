# To run, first set the correct db connections in server.R and ui.R (lines starting with "db <- db.connect(host="), set the correct table names (replace "shiny.x" table names with your table names), and set the correct plot.path to save plots (server.R line starting with "plot.path <-").  Then, in the R console, set the working directory (setwd("/my/working/directory")) to the folder containing ui.R and server.R and run the following two lines:
## library(shiny)
## runApp(".")

library(shiny)
library(shinyIncubator) # for progress bar and matrix input
library(PivotalR)

db = db.connect(host="localhost",user="gpadmin", password="changeme",dbname="gpadmin")
sample <- db.data.frame("raw_data", conn.id=db)

# Get list of input values
type <- lookat(unique(sample$type), -1)
week <- lookat(unique(sample$week),-1)
db.disconnect(conn.id=db)
drivers.planned.names <- c("Assortment", "Price", "% Items on Promo", 
  "% Items on Redline")

shinyUI(

  navbarPage("Sales Decomposition App", 

  ################### Historic Sales Data tab ##########################
  tabPanel("Historic Sales Data",
    pageWithSidebar(
      # Title
      headerPanel(
        list(
          tags$head(
            tags$style(".span12 {color: #333333; vertical-align:bottom}")
            , tags$style("h2 {color: #333333; font-size:30px; font-family: \"
              Lucida Sans Unicode\", \"Lucida Grande\", sans-serif; 
              vertical-align:bottom; }")
          ),
        HTML('<h2>Historic Sales</h2>')
        )
      ), # end headerPanel

      # Inputs
      sidebarPanel(
        tags$style(type='text/css', ".row-fluid .span4{width: 25%;}"),
        tags$style(type='text/css', "h1 { color: #333333;}"),
        tags$style(type='text/css', ".well { background-color: #FFFFFF; color: #333333}"),

        tags$style(type='text/css', "p { background-color: #FFFFFF; color: #333333}"),
        tags$style(type='text/css', "body { background-color: #FFFFFF; color: #333333}"),
        tags$style(type='text/css', ".control-label {color: #333333}"),
        tags$style(type='text/css', "h4 { color: #333333;}"),
        tags$style(type='text/css', "h5 { background-color: #F3E8A4;}"),
        h4(strong("Plotting Parameters")),
        helpText("Select parameters for plotting."),
    
        selectInput("plot.brand", "Brand:", 
              c("Brand 1", "Brand 2")),
        selectInput("plot.gender", "Gender:", 
              c("Female", "Male")),
        uiOutput("ChoosePlotDept"),
        selectInput("plot.type","Store Type:",
              c("- All -", type)), 
        uiOutput("ChoosePlotPeriod"), 
    
        wellPanel(
          h4("Plots"),
          checkboxInput(inputId = "check.plot.blended.units", label = "Total units sold 
            vs. price"),
          checkboxInput(inputId = "check.plot.non.promo.units", label = "Non-promo units 
            sold vs. price"),
          checkboxInput(inputId = "check.plot.promo.units", label = "Promo units sold vs. 
            price"),
          checkboxInput(inputId = "check.plot.stacked.units", label = "Total units sold by 
            markdown, promo")
        ),
        
        conditionalPanel(
          condition = "input.check.plot.blended.units==true | 
            input.check.plot.non.promo.units==true | input.check.plot.promo.units==true",
          wellPanel(
            h4("Markdowns"),
            checkboxInput(inputId = "check.plot.separate", label = "Plot non-redline and 
              redline items separately", TRUE)
          )
        ),
      
        # Option for smoothed pricing since aggregating and dept/type/week level could 
        # result in widely varying prices week over week (depending on which items sold 
        # more, since price is calculated as =revenue/qty), which does't accurately 
        # capture how consumers think about regular/ticket price (e.g. expected to be more 
        # constant)
        wellPanel(
          h4("Prices"),
          checkboxInput(inputId = "check.plot.price.smooth", label = "Use smoothed 
            pricing", TRUE),
          conditionalPanel(condition = "input.check.plot.price.smooth == true", 
            helpText("Smoothed pricing: weeks with zero sales will have prices imputed 
            from the most recent valid (non-zero) value, plus Friedman's Super Smoother 
            smoothing."))
        ),
  
        wellPanel(
          h4("Data"),
          checkboxInput(inputId = "check.raw.data", label = "Display raw data table"),
          conditionalPanel("input.check.raw.data==true", checkboxInput(inputId = 
            "pageable", label = "Choose display page size")),
          # arbitrary max page value
          conditionalPanel("pageable==true", numericInput(inputId = "pagesize", 
            label = "Weeks per page",10, max=200))
        ),
  
        actionButton("run.plots", "Generate Plots >>>")      
      ),

      # Show a tabset that includes plots and table view
      mainPanel(
        tags$style(type='text/css', "h1 { color: #333333;}"),
        tags$style(type='text/css', "h5 { color: #990000;}"),
        tags$style(type='text/css', "body { color: #333333}"),
  
        progressInit(),
        tabsetPanel(
          tabPanel("Stacked Sales Plot", uiOutput("stacked.units.plot")), 
          tabPanel("Sales Units by Week", uiOutput("sales.units.plot")),
          tabPanel("Raw Data",  htmlOutput("raw.table"))
        )
      )
    )
  ), 
  
  ############## Historic Sales Decomp tab ######################
  tabPanel("Historic Sales Decomposition",
    pageWithSidebar(
      # Title
      headerPanel(
        list(
          tags$head(
            tags$style(".span12 {color: #333333; vertical-align:bottom}")
            ,tags$style("h2 {color: #333333; font-size:30px; font-family: \"Lucida Sans 
              Unicode\", \"Lucida Grande\", sans-serif; vertical-align:bottom; }")
          ),
          HTML('<h2>Historic Sales Decomposition</h2>')
        )
      ),

      # Inputs
      sidebarPanel(
        tags$style(type='text/css', ".row-fluid .span4{width: 25%;}"),
        tags$style(type='text/css', "h1 { color: #333333;}"),
        tags$style(type='text/css', ".well { background-color: #FFFFFF; color: #333333}"),
  
        tags$style(type='text/css', "p { background-color: #FFFFFF; color: #333333}"),
        tags$style(type='text/css', "body { background-color: #FFFFFF; color: #333333}"),
        tags$style(type='text/css', ".control-label {color: #333333}"),
        tags$style(type='text/css', "h4 { color: #333333;}"),
        tags$style(type='text/css', "h5 { background-color: #F3E8A4;}"),
        helpText("Select parameters for historic sales decomp."),
        selectInput("historic.decomp.brand", "Brand:", 
              c("Brand 1", "Brand 2")),
        selectInput("historic.decomp.gender", "Gender:", 
              c("Female", "Male")),
        uiOutput("ChooseHistoricDecompDept"),
        selectInput("historic.decomp.type","Store Type:",
              type),
        uiOutput("ChooseHistoricPeriod"),
        selectInput("historic.decomp.group", "View decomp plot per:", 
              c("Entire period", "Quarter", "Month")),
      
        actionButton("run.historic.decomp", "Run Decomp Analysis >>>")     
      ),
  
      # Show a tabset that includes a plots and table view
      mainPanel(
        tags$style(type='text/css', "h1 { color: #333333;}"),
        tags$style(type='text/css', "h5 { color: #990000;}"),
        tags$style(type='text/css', "body { color: #333333}"),
  
        progressInit(),
        tabsetPanel(
          tabPanel("Sales Decomp Bar Chart", uiOutput("find.historic.decomp.plot")),
          tabPanel("Sales Decomp % Breakout",  htmlOutput("historic.decomp.pct")),
          tabPanel("Sales Decomp Raw Data",  htmlOutput("historic.decomp.data"))
        )
      )
    )
  ),

  ############################## What-If Scenarios tab #################################
  tabPanel("What-If Scenario Analysis",
    fluidPage(
      tags$style(type='text/css', '#predictions {height: 200px}'),

      tabsetPanel(
          
        tabPanel("Simulated Sales Prediction", htmlOutput("predictions")), 
        tabPanel("Simulated Sales Decomp", plotOutput("predictions.decomp")),
        tabPanel("Simulated Sales Decomp % Breakout", 
          htmlOutput("predictions.decomp.pct")),
        tabPanel("Simulated Sales Raw Data", htmlOutput("predictions.data"))
      ),
      
      hr(),
      
      fluidRow(
        column(12,
          # Title
          tags$style(".span12 {color: #333333; vertical-align:bottom}"),
          tags$style("h2 {color: #333333; font-size:30px; font-family: \"Lucida Sans 
            Unicode\", \"Lucida Grande\", sans-serif; vertical-align:bottom; }"),
          h2("What-If Scenario Analysis"),

          # Inputs
          tags$style(type='text/css', "p { background-color: #FFFFFF; color: #333333}"),
          tags$style(type='text/css', "body { background-color: #FFFFFF; color: 
            #333333}"),
          tags$style(type='text/css', ".control-label {color: #333333}"),
          helpText("Select parameters for the what-if scenario."),
          
          fluidRow(
            column(2,
              selectInput("whatif.brand", "Brand:", c("Brand 1", "Brand 2"))
              ), 
            column(1,
              selectInput("whatif.gender", "Gender:", c("Female", "Male"))
              ), 
            column(3,
              uiOutput("ChooseWhatIfDept")
              ), 
            column(1,
              selectInput("whatif.type","Store Type:", type)
              ), 
            column(3,
              uiOutput("ChooseWhatIfPeriod")
              ), 
            column(2,
              selectInput("whatif.decomp.group", "View decomp plot per:", c("Entire 
                period", "Quarter", "Month"))
              )
          ),
          
          fluidRow(column(12,actionButton("run.whatif.scenario", "Run Scenario >>>"))),
          fluidRow(column(12,""))
          
          ,fluidRow(
            column(3,
              selectInput("update.driver","Fast Update Driver:", drivers.planned.names)
              ), 
            column(2,
              numericInput("update.num", "Increase/Decrease Amount:", 1)
              ), 
            column(1,
              fluidRow(column(12,"")),
              fluidRow(column(6, actionButton("run.add", "+")), column(6, 
                actionButton("run.subtract", "-")))
              ), 
            column(6,"")
          )
           
        )
      ), # end fluidrow
        
      fluidRow(  
        column(12, 
          # input matrix for planners to enter scenario features
          tags$head(
            tags$style(type='text/css'
              , ".foo {background-color: rgb(255,255,255);}" # make inside of cells white
              , ".table th, .table td{text-align: center;}" # works except for colnames
              , ".tableinput .hide {display: table-header-group;}" # un-hide colnames
            )
          ),
          uiOutput("planner.input")
        )
      ) # end fluidrow
    ) # end fluidpage
  )
))
