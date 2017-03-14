data = read.csv('/Users/mauricezeitouni/Desktop/Kartees_Data_Collection/2016_data/Mets/consolidated_datasets/2017-03-08_08_47/sections_future_past_days.csv', header = TRUE)

model = lm(data$three_days_ahead_price ~ data$Zone_Section_Average_Price + data$Time_Diff + factor(data$Zone) + data$Total_Tickets + data$Average_Price + data$Zone_Section_Total_Tickets + data$Zone_Section_Min_Price + data$Zone_Section_Max_Price + data$Zone_Section_Std + factor(data$Tier))
summary(model)
