SELECT 
    number,
    Month,
    `Payment amount`,
    ROUND(`Payment of the principal debt`) AS `Payment of the principal debt-new`,
    ROUND(`Payment of interest`) AS `Payment of interest-new`,
    `Balance of debt`,
    `interest`,
    `debt`
FROM 
    DZ_7;