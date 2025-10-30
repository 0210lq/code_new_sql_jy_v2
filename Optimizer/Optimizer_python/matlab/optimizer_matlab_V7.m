function optimizer_matlab_V7(n,varargin)
filePath = varargin{1};
filePath
% 打开文本文件
fileID = fopen(filePath, 'r');
%
% �?查文件是否成功打�?
if fileID == -1
    error('File cannot be opened');
end

% 逐行读取文件内容
while ~feof(fileID)
    % 读取�?�?
    line = fgetl(fileID);
    % 去除可能的换行符
    line = strtrim(line);
    disp(line);
    % 调用函数处理每一�?
    main(line);
end
% 关闭文件
fclose(fileID);

    function main(path)
        style_factor = readtable(fullfile(path,'parameter_selecting.xlsx'),'Sheet','style');
        style_len = size(style_factor,1);
        industry_factor = readtable(fullfile(path,'parameter_selecting.xlsx'),'Sheet','industry');
        industry_len = size(industry_factor,1);
        stock_risk = importdata(fullfile(path,'Stock_risk_exposure.csv'));
        stock_risk = stock_risk.data;
        stock_risk(isnan(stock_risk)) = 0;
        stock_risk(isinf(stock_risk)) = 0;
        index_risk = importdata(fullfile(path,'index_risk_exposure.csv'));
        index_risk = index_risk.data;
        index_risk(isnan(index_risk)) = 0;
        index_risk(isinf(index_risk)) = 0;
        stock_score = importdata(fullfile(path,'Stock_score.csv'));
        stock_score = stock_score.data;
        initial_weight = importdata(fullfile(path,'Stock_initial_weight.csv'));
        initial_weight = initial_weight.data;
        lower_weight = importdata(fullfile(path,'Stock_lower_weight.csv'));
        lower_weight = lower_weight.data;
        upper_weight = importdata(fullfile(path,'Stock_upper_weight.csv'));
        upper_weight = upper_weight.data;
        index_initial_weight = importdata(fullfile(path,'index_initial_weight.csv'));
        index_initial_weight = index_initial_weight.data;
        stock_sperisk = importdata(fullfile(path,'Stock_specific_risk.csv'));
        stock_sperisk = stock_sperisk.data;
        factor_cov = importdata(fullfile(path,'factor_cov.csv'));
        factor_cov =factor_cov.data;
        % factor_cov=factor_cov(1:style_len,1:style_len);
        factor_constraint_upper = importdata(fullfile(path,'factor_constraint_upper.csv'));
        factor_constraint_upper = factor_constraint_upper.data;
        te_value = factor_constraint_upper(1);
        factor_constraint_upper = factor_constraint_upper(2:end)';
        factor_constraint_lower = importdata(fullfile(path,'factor_constraint_lower.csv'));
        factor_constraint_lower = factor_constraint_lower.data;
        factor_constraint_lower = factor_constraint_lower(2:end)';
        stock_number = size(stock_score,1);
        final_weight = [];
        style_weight_upper = factor_constraint_upper(1:style_len,:);
        style_weight_lower = factor_constraint_lower(1:style_len,:);
        industry_weight_upper = factor_constraint_upper(style_len+1:end,:);
        industry_weight_lower = factor_constraint_lower(style_len+1:end,:);
        barra_stock_risk = stock_risk(:,1:style_len);
        industry_stock_risk = stock_risk(:,style_len+1:end);
        barra_index_risk = index_risk(:,1:style_len);
        industry_index_risk = index_risk(:,style_len+1:end);
        V=(stock_risk*factor_cov*stock_risk'+diag(stock_sperisk.^2));
        score_stock=stock_score';
        f=@(x) -score_stock *x;
        x0 = initial_weight;
        lb = lower_weight;
        ub = upper_weight;
        x0 = max(min(x0, ub), lb);
        nonlcon=@(x)fun2(x,stock_risk,index_risk,style_weight_upper,style_weight_lower,industry_weight_upper,industry_weight_lower,V,index_initial_weight,te_value,style_len);
        A = []; b = []; Aeq =ones(1,stock_number) ; beq = ones(1,1);
        options = optimoptions('fmincon','Display','iter','Algorithm','sqp','MaxFunctionEvaluations',200000, 'UseParallel', false); % Set options
        %options = optimoptions('fmincon', 'Display','iter', 'MaxFunctionEvaluations', 2000000, 'Algorithm', 'inferior-point', 'UseParallel', true);
        x = fmincon(f,x0,A,b,Aeq,beq,lb,ub,nonlcon,options);
        portfolio_risk=sqrt((x-index_initial_weight)'*V*(x-index_initial_weight)*252);
        portfolio_barra_risk = barra_stock_risk' * x;
        portfolio_industry_risk = industry_stock_risk' * x;
        final_score=score_stock *x;
        final_weight=[final_weight,x];
        weight_sum = sum(final_weight);
        industry_risk_ratio = (portfolio_industry_risk - industry_index_risk') ./ abs(industry_index_risk');
        barra_saving_info = [portfolio_barra_risk, barra_index_risk', ...
            (portfolio_barra_risk - barra_index_risk') ./ abs(barra_index_risk'), ...
            repmat(portfolio_risk, size(portfolio_barra_risk)), repmat(final_score, size(portfolio_barra_risk)), repmat(weight_sum, size(portfolio_barra_risk))];
        industry_saving_info = [portfolio_industry_risk, industry_index_risk', industry_risk_ratio];
        csv1 = fullfile(path, 'weight.csv');
        csv2 = fullfile(path, 'barra_risk.csv');
        csv3 = fullfile(path, 'industry_risk.csv');
        csvwrite(csv1, final_weight);
        csvwrite(csv2, barra_saving_info);
        csvwrite(csv3, industry_saving_info);
        
        function [g,h]=fun2(x,stock_risk,index_risk,style_weight_upper,style_weight_lower,industry_weight_upper,industry_weight_lower,V,index_initial_weight,TE,style_len)
            barra_stock_risk=stock_risk(:,1:style_len);
            industry_stock_risk=stock_risk(:,style_len+1:end);
            barra_index_risk=index_risk(:,1:style_len);
            industry_index_risk=index_risk(:,style_len+1:end);
            portfolio_barra_risk=barra_stock_risk'*x;
            portfolio_industry_risk=industry_stock_risk'*x;
            g1_upper=portfolio_barra_risk-barra_index_risk'-style_weight_upper.*abs(barra_index_risk');
            g1_lower=-(portfolio_barra_risk-barra_index_risk')+style_weight_lower.*abs(barra_index_risk');
            g2_upper=portfolio_industry_risk-industry_index_risk'-industry_weight_upper.*abs(industry_index_risk');
            g2_lower=-(portfolio_industry_risk-industry_index_risk')+industry_weight_lower.*abs(industry_index_risk');
            g3=sqrt((x-index_initial_weight)'*V*(x-index_initial_weight)*252)-TE;
            g=[g1_upper;g1_lower;g2_upper;g2_lower;g3];
            h=[];
        end
    end
txt_name = sprintf('result%d.txt',n);
filepath = fullfile(pwd,txt_name);
fileID = fopen(filepath,'w');
if fileID == -1
    error('File cannot be opened');
end
fprintf(fileID,'%d',1);
fclose(fileID);
exit(0);
end


