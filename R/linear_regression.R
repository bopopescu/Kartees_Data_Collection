data = read.csv('/Users/mauricezeitouni/Desktop/Kartees_Data_Collection/2016_data/Mets/consolidated_datasets/2017-03-07_14_09/sections_clean_time_l10_factored.csv', header = TRUE)

model = lm(data$Zone_Section_Average_Price ~ data$Time_Diff + factor(data$Zone) + data$Total_Tickets + data$Average_Price + data$Zone_Section_Total_Tickets + data$Zone_Section_Min_Price + data$Zone_Section_Max_Price + data$Zone_Section_Std + data$Wins + data$Losses+data$L_10 + factor(data$Tier))

summary(model)