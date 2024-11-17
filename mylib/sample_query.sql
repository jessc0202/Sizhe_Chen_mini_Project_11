SELECT 
    country,
    AVG(beer_servings) AS avg_beer_servings,
    AVG(spirit_servings) AS avg_spirit_servings,
    AVG(wine_servings) AS avg_wine_servings,
    AVG(total_litres_of_pure_alcohol) AS avg_total_alcohol,
    COUNT(*) AS total_entries
FROM 
    drinks
GROUP BY 
    country
ORDER BY 
    avg_total_alcohol DESC;
