function [final_weight, barra_saving_info, industry_saving_info] = optimizer_matlab_func_v2(path,path_yes,worker_count)
% 函数功能：读取指定路径下的数据文件，进行投资组合优化
% path：输入路径，包含?有必要的CSV数据文件
% worker_count：并行工作进程数量，默认从配置文件读?
% path = 'd:/Optimizer_python_data_test/processing_data/fm01_hs300_HB/2025-02-05'
% 使用持久变量保持并行池在函数调用之间存在
persistent persistentPool;

% 记录?始时?
tic;

% 打印接收到的路径参数
fprintf('MATLAB函数?始处理路?: %s\n', path);

try
        % 如果配置文件不存在，使用默认配置
        fprintf('配置文件不存在，使用默认配置\n');
        config = struct();
        config.worker_count_default = 6;
        config.input_files = struct();
        config.input_files.parameter_selecting = 'parameter_selecting.xlsx';
        config.input_files.stock_risk = 'Stock_risk_exposure.csv';
        config.input_files.index_risk = 'index_risk_exposure.csv';
        config.input_files.stock_score = 'Stock_score.csv';
        config.input_files.stock_code = 'Stock_code.csv';
        config.input_files.initial_weight = 'Stock_initial_weight.csv';
        config.input_files.lower_weight = 'Stock_lower_weight.csv';
        config.input_files.upper_weight = 'Stock_upper_weight.csv';
        config.input_files.index_initial_weight = 'index_initial_weight.csv';
        config.input_files.stock_specific_risk = 'Stock_specific_risk.csv';
        config.input_files.factor_cov = 'factor_cov.csv';
        config.input_files.factor_constraint_upper = 'factor_constraint_upper.csv';
        config.input_files.factor_constraint_lower = 'factor_constraint_lower.csv';
        config.output_files = struct();
        config.output_files.weight = 'weight.csv';
        config.output_files.barra_risk = 'barra_risk.csv';
        config.output_files.industry_risk = 'industry_risk.csv';
        config.optimization_params = struct();
        config.optimization_params.Algorithm = 'sqp';
        config.optimization_params.MaxFunctionEvaluations = 200000;
        config.optimization_params.Display = 'iter';
        config.optimization_params.UseParallel = true;
    
    % 设置工作进程数量
    if nargin < 2 || isempty(worker_count)
        worker_count = config.worker_count_default;
        fprintf('使用配置文件中的默认并行工作进程数量: %d\n', worker_count);
    else
        worker_count = double(worker_count);
        fprintf('使用指定的并行工作进程数?: %d\n', worker_count);
    end
    
    % 配置并行?
    fprintf('配置并行计算，最大工作进程数?: %d\n', worker_count);
    %?查并行池是否已经存在且有?
    if isempty(persistentPool) || ~isvalid(persistentPool) || persistentPool.NumWorkers ~= worker_count
        %?查是否有其他并行?
        poolobj = gcp('nocreate');
        if ~isempty(poolobj)
            % 关闭现有并行?
            fprintf('关闭现有并行池（%d个工作进程）\n', poolobj.NumWorkers);
            delete(poolobj);
        end
        % 创建新的并行池并存储在持久变量中
        fprintf('创建新并行池?%d个工作进程\n', worker_count);
        persistentPool = parpool('local', worker_count);
    else
        % 重用现有并行?
        fprintf('复用现有并行池，%d个工作进程\n', persistentPool.NumWorkers);
    end
    
    % 读取数据
    fprintf('?始读取数?...\n');
    
    % 读取风格因子和行业因?
    try
        style_factor = readtable(fullfile(path, config.input_files.parameter_selecting), 'Sheet', 'style');
        style_len = size(style_factor, 1);
        fprintf('风格因子数量: %d\n', style_len);
        fprintf('风格因子表类?: %s, 维度: %dx%d\n', class(style_factor), size(style_factor, 1), size(style_factor, 2));
        
        industry_factor = readtable(fullfile(path, config.input_files.parameter_selecting), 'Sheet', 'industry');
        industry_len = size(industry_factor, 1);
        fprintf('行业因子数量: %d\n', industry_len);
        fprintf('行业因子表类?: %s, 维度: %dx%d\n', class(industry_factor), size(industry_factor, 1), size(industry_factor, 2));
    catch e
        fprintf('读取因子数据失败: %s，使用默认?\n', e.message);
        style_len = 9;  % 默认风格因子数量
        industry_len = 30;  % 默认行业因子数量
        fprintf('使用默认风格因子数量: %d, 行业因子数量: %d\n', style_len, industry_len);
    end
    
    % 读取股票风险暴露
    stock_risk_path = fullfile(path, config.input_files.stock_risk);
    fprintf('读取股票风险暴露: %s\n', stock_risk_path);
    stock_risk = importdata(stock_risk_path);
    fprintf('股票风险暴露原始数据类型: %s\n', class(stock_risk));
    if isstruct(stock_risk)
        fprintf('股票风险暴露包含字段: %s\n', strjoin(fieldnames(stock_risk), ', '));
        stock_risk = stock_risk.data;
    end
    fprintf('股票风险暴露数据类型: %s, 维度: %dx%d\n', class(stock_risk), size(stock_risk, 1), size(stock_risk, 2));
    stock_risk(isnan(stock_risk)) = 0;
    stock_risk(isinf(stock_risk)) = 0;
    
    % 读取指数风险暴露
    index_risk_path = fullfile(path, config.input_files.index_risk);
    fprintf('读取指数风险暴露: %s\n', index_risk_path);
    index_risk = importdata(index_risk_path);
    fprintf('指数风险暴露原始数据类型: %s\n', class(index_risk));
    if isstruct(index_risk)
        fprintf('指数风险暴露包含字段: %s\n', strjoin(fieldnames(index_risk), ', '));
        index_risk = index_risk.data;
    end
    fprintf('指数风险暴露数据类型: %s, 维度: %dx%d\n', class(index_risk), size(index_risk, 1), size(index_risk, 2));
    index_risk(isnan(index_risk)) = 0;
    index_risk(isinf(index_risk)) = 0;
    
    % 读取股票分数
    stock_score_path = fullfile(path, config.input_files.stock_score);
    fprintf('读取股票分数: %s\n', stock_score_path);
    stock_score = importdata(stock_score_path);
    fprintf('股票分数原始数据类型: %s\n', class(stock_score));
    if isstruct(stock_score)
        fprintf('股票分数包含字段: %s\n', strjoin(fieldnames(stock_score), ', '));
        stock_score = stock_score.data;
    end
    fprintf('股票分数数据类型: %s, 维度: %dx%d\n', class(stock_score), size(stock_score, 1), size(stock_score, 2));
    % 读取权重下限
    lower_weight_path = fullfile(path, config.input_files.lower_weight);
    fprintf('读取权重下限: %s\n', lower_weight_path);
    lower_weight = importdata(lower_weight_path);
    fprintf('权重下限原始数据类型: %s\n', class(lower_weight));
    if isstruct(lower_weight)
        fprintf('权重下限包含字段: %s\n', strjoin(fieldnames(lower_weight), ', '));
        lower_weight = lower_weight.data;
    end
    fprintf('权重下限数据类型: %s, 维度: %dx%d\n', class(lower_weight), size(lower_weight, 1), size(lower_weight, 2));
    
    % 读取权重上限
    upper_weight_path = fullfile(path, config.input_files.upper_weight);
    fprintf('读取权重上限: %s\n', upper_weight_path);
    upper_weight = importdata(upper_weight_path);
    fprintf('权重上限原始数据类型: %s\n', class(upper_weight));
    if isstruct(upper_weight)
        fprintf('权重上限包含字段: %s\n', strjoin(fieldnames(upper_weight), ', '));
        upper_weight = upper_weight.data;
    end
    fprintf('权重上限数据类型: %s, 维度: %dx%d\n', class(upper_weight), size(upper_weight, 1), size(upper_weight, 2));
    
    % 读取指数初始权重
    index_initial_weight_path = fullfile(path, config.input_files.index_initial_weight);
    fprintf('读取指数初始权重: %s\n', index_initial_weight_path);
    index_initial_weight = importdata(index_initial_weight_path);
    fprintf('指数初始权重原始数据类型: %s\n', class(index_initial_weight));
    if isstruct(index_initial_weight)
        fprintf('指数初始权重包含字段: %s\n', strjoin(fieldnames(index_initial_weight), ', '));
        index_initial_weight = index_initial_weight.data;
    end
    fprintf('指数初始权重数据类型: %s, 维度: %dx%d\n', class(index_initial_weight), size(index_initial_weight, 1), size(index_initial_weight, 2));
    
    % 读取股票特异风险
    stock_specific_risk_path = fullfile(path, config.input_files.stock_specific_risk);
    fprintf('读取股票特异风险: %s\n', stock_specific_risk_path);
    stock_sperisk = importdata(stock_specific_risk_path);
    fprintf('股票特异风险原始数据类型: %s\n', class(stock_sperisk));
    if isstruct(stock_sperisk)
        fprintf('股票特异风险包含字段: %s\n', strjoin(fieldnames(stock_sperisk), ', '));
        stock_sperisk = stock_sperisk.data;
    end
    fprintf('股票特异风险数据类型: %s, 维度: %dx%d\n', class(stock_sperisk), size(stock_sperisk, 1), size(stock_sperisk, 2));
    
    % 读取因子协方差
    factor_cov_path = fullfile(path, config.input_files.factor_cov);
    fprintf('读取因子协方差: %s\n', factor_cov_path);
    factor_cov = importdata(factor_cov_path);
    fprintf('因子协方差原始数据类型: %s\n', class(factor_cov));
    if isstruct(factor_cov)
        fprintf('因子协方差包含字段: %s\n', strjoin(fieldnames(factor_cov), ', '));
        factor_cov = factor_cov.data;
    end
    fprintf('因子协方差数据类型: %s, 维度: %dx%d\n', class(factor_cov), size(factor_cov, 1), size(factor_cov, 2));
    
    % 读取因子约束上限
    factor_constraint_upper_path = fullfile(path, config.input_files.factor_constraint_upper);
    fprintf('读取因子约束上限: %s\n', factor_constraint_upper_path);
    factor_constraint_upper = importdata(factor_constraint_upper_path);
    fprintf('因子约束上限原始数据类型: %s\n', class(factor_constraint_upper));
    if isstruct(factor_constraint_upper)
        fprintf('因子约束上限包含字段: %s\n', strjoin(fieldnames(factor_constraint_upper), ', '));
        factor_constraint_upper = factor_constraint_upper.data;
    end
    fprintf('因子约束上限数据类型: %s, 维度: %dx%d\n', class(factor_constraint_upper), size(factor_constraint_upper, 1), size(factor_constraint_upper, 2));
    te_value = factor_constraint_upper(1);
    fprintf('跟踪误差约束: %f\n', te_value);
    factor_constraint_upper = factor_constraint_upper(2:end)';
    fprintf('处理后的因子约束上限维度: %dx%d\n', size(factor_constraint_upper, 1), size(factor_constraint_upper, 2));
    
    % 读取因子约束下限
    factor_constraint_lower_path = fullfile(path, config.input_files.factor_constraint_lower);
    fprintf('读取因子约束下限: %s\n', factor_constraint_lower_path);
    factor_constraint_lower = importdata(factor_constraint_lower_path);
    fprintf('因子约束下限原始数据类型: %s\n', class(factor_constraint_lower));
    if isstruct(factor_constraint_lower)
        fprintf('因子约束下限包含字段: %s\n', strjoin(fieldnames(factor_constraint_lower), ', '));
        factor_constraint_lower = factor_constraint_lower.data;
    end
    fprintf('因子约束下限数据类型: %s, 维度: %dx%d\n', class(factor_constraint_lower), size(factor_constraint_lower, 1), size(factor_constraint_lower, 2));
    factor_constraint_lower = factor_constraint_lower(2:end)';
    fprintf('处理后的因子约束下限维度: %dx%d\n', size(factor_constraint_lower, 1), size(factor_constraint_lower, 2));
    
    % 计算股票数量
    stock_number = size(stock_score, 1);
    fprintf('股票数量: %d\n', stock_number);
    
    % 检查因子约束向量长度
    if length(factor_constraint_upper) < (style_len + industry_len)
        fprintf('警告: 因子约束上限向量长度不足，需%d，实际为%d\n', style_len + industry_len, length(factor_constraint_upper));
        % 扩展向量长度
        expanded = zeros(style_len + industry_len, 1);
        expanded(1:length(factor_constraint_upper)) = factor_constraint_upper;
        factor_constraint_upper = expanded;
        fprintf('已扩展因子约束上限向量长度至%d\n', length(factor_constraint_upper));
    end
    
    if length(factor_constraint_lower) < (style_len + industry_len)
        fprintf('警告: 因子约束下限向量长度不足，需%d，实际为%d\n', style_len + industry_len, length(factor_constraint_lower));
        % 扩展向量长度
        expanded = zeros(style_len + industry_len, 1);
        expanded(1:length(factor_constraint_lower)) = factor_constraint_lower;
        factor_constraint_lower = expanded;
        fprintf('已扩展因子约束下限向量长度至%d\n', length(factor_constraint_lower));
    end
    % 分离风格权重和行业权重约束
    style_weight_upper = factor_constraint_upper(1:style_len, :);
    style_weight_lower = factor_constraint_lower(1:style_len, :);
    industry_weight_upper = factor_constraint_upper(style_len+1:end, :);
    industry_weight_lower = factor_constraint_lower(style_len+1:end, :);
    fprintf('风格权重上限维度: %dx%d\n', size(style_weight_upper, 1), size(style_weight_upper, 2));
    fprintf('风格权重下限维度: %dx%d\n', size(style_weight_lower, 1), size(style_weight_lower, 2));
    fprintf('行业权重上限维度: %dx%d\n', size(industry_weight_upper, 1), size(industry_weight_upper, 2));
    fprintf('行业权重下限维度: %dx%d\n', size(industry_weight_lower, 1), size(industry_weight_lower, 2));
    
    % 检查股票风险暴露维度
    if size(stock_risk, 2) < (style_len + industry_len)
        fprintf('警告: 股票风险暴露维度不足，需%d列，实际只有%d列\n', style_len + industry_len, size(stock_risk, 2));
    end
    
    % 检查指数风险暴露维度
    if size(index_risk, 2) < (style_len + industry_len)
        fprintf('警告: 指数风险暴露维度不足，需%d列，实际只有%d列\n', style_len + industry_len, size(index_risk, 2));
    end
    
    % 分离风格风险和行业风险暴露
    barra_stock_risk = stock_risk(:, 1:style_len);
    industry_stock_risk = stock_risk(:, style_len+1:end);
    barra_index_risk = index_risk(:, 1:style_len);
    industry_index_risk = index_risk(:, style_len+1:end);
    fprintf('风格风险暴露维度: %dx%d\n', size(barra_stock_risk, 1), size(barra_stock_risk, 2));
    fprintf('行业风险暴露维度: %dx%d\n', size(industry_stock_risk, 1), size(industry_stock_risk, 2));
    fprintf('指数风格风险暴露维度: %dx%d\n', size(barra_index_risk, 1), size(barra_index_risk, 2));
    fprintf('指数行业风险暴露维度: %dx%d\n', size(industry_index_risk, 1), size(industry_index_risk, 2));
    
    % 计算协方差矩阵
    fprintf('计算协方差矩阵...\n');
    V = (stock_risk * factor_cov * stock_risk' + diag(stock_sperisk.^2));
    fprintf('协方差矩阵维度: %dx%d\n', size(V, 1), size(V, 2));
            % 读取初始权重
    initial_code_path = fullfile(path, config.input_files.stock_code);
    initial_weight_path = fullfile(path, config.input_files.initial_weight);
    initial_weight_path_yes = fullfile(path_yes, config.output_files.weight);
    initial_code_yes_path = fullfile(path_yes, config.input_files.stock_code);
    fprintf('读取初始权重: %s\n', initial_weight_path);
    initial_weight = importdata(initial_weight_path);
    fprintf('初始权重原始数据类型: %s\n', class(initial_weight));
    if isstruct(initial_weight)
        fprintf('初始权重包含字段: %s\n', strjoin(fieldnames(initial_weight), ', '));
        initial_weight = initial_weight.data;
    end
    fprintf('初始权重数据类型: %s, 维度: %dx%d\n', class(initial_weight), size(initial_weight, 1), size(initial_weight, 2));

    % 检查是否存在yes文件并应用权重约束
    if exist(initial_weight_path_yes, 'file') && exist(initial_code_yes_path, 'file')
        fprintf('检测到yes文件，应用权重约束...\n');
        % 读取yes权重文件
        initial_weight_yes = importdata(initial_weight_path_yes);
        if isstruct(initial_weight_yes)
            initial_weight_yes = initial_weight_yes.data;
        end
        
        % 读取yes代码文件
        initial_code_yes = readtable(initial_code_yes_path);
        % 转换为cell数组并去掉第一行（日期行）
        initial_code_yes = table2cell(initial_code_yes(2:end, :));
        
        % 确保代码和权重维度匹配
        if size(initial_code_yes, 1) == size(initial_weight_yes, 1)
            % 打印调试信息
            fprintf('initial_code_yes 类型: %s\n', class(initial_code_yes));
            fprintf('initial_code_yes 第一行: ');
            disp(initial_code_yes(1,:));
            fprintf('initial_weight_yes 类型: %s\n', class(initial_weight_yes));
            fprintf('initial_weight_yes 第一行: ');
            disp(initial_weight_yes(1,:));
            
            % 创建代码到权重的映射
            code_weight_map = containers.Map(initial_code_yes, initial_weight_yes);
            
            % 获取当前权重对应的代码
            current_codes = readtable(initial_code_path);
            % 转换为cell数组并去掉第一行（日期行）
            current_codes = table2cell(current_codes(2:end, 1)); % 只取第一列作为代码
            
            % 打印调试信息
            fprintf('current_codes 类型: %s\n', class(current_codes));
            fprintf('current_codes 第一行: ');
            disp(current_codes{1});
            
            % 创建final_initial_weight并初始化为initial_weight
            final_initial_weight = initial_weight;
            
            % 应用权重约束
            for i = 1:length(current_codes)
                code = current_codes{i};  % 使用花括号访问cell数组
                if isKey(code_weight_map, code)
                    final_initial_weight(i) = code_weight_map(code); % 如果在yes列表中，使用yes列表中的权重
                end
                % 如果代码不在yes列表中，保持原始权重不变
            end
            
            % 检查跟踪误差计算
            diff_weight = final_initial_weight - index_initial_weight;
            V_diff = V * diff_weight;
            quad_term = diff_weight' * V_diff;
            annual_term = quad_term * 252;
            
            % 如果年化项小于0，回退到原始权重
            if annual_term < 0
                fprintf('警告：应用yes权重后年化跟踪误差项为负（%f），回退到原始权重\n', annual_term);
                final_initial_weight = initial_weight;
            else
                fprintf('应用yes权重后年化跟踪误差项为：%f\n', annual_term);
            end
            
            fprintf('权重约束应用完成，最终权重维度: %dx%d\n', size(final_initial_weight, 1), size(final_initial_weight, 2));
        else
            warning('yes文件中的代码和权重维度不匹配，跳过权重约束');
            final_initial_weight = initial_weight;
        end
    else
        fprintf('未检测到yes文件，跳过权重约束\n');
        final_initial_weight = initial_weight;
    end

    % 设置目标函数
    score_stock = stock_score';
    fprintf('转置后的股票分数维度: %dx%d\n', size(score_stock, 1), size(score_stock, 2));
    f = @(x) -score_stock * x;
    
    % 设置初始值和约束
    x0 = final_initial_weight;
    lb = lower_weight;
    ub = upper_weight;
    x0 = max(min(x0, ub), lb);
    fprintf('初始解维度: %dx%d\n', size(x0, 1), size(x0, 2));
    fprintf('下界维度: %dx%d\n', size(lb, 1), size(lb, 2));
    fprintf('上界维度: %dx%d\n', size(ub, 1), size(ub, 2));
    
    % 在调用 fmincon 之前添加初始点验证
    fprintf('验证初始点:\n');
    fprintf('初始点 x0 中的 NaN: %d, Inf: %d\n', sum(isnan(x0(:))), sum(isinf(x0(:))));
    fprintf('初始点 x0 范围: [%f, %f]\n', min(x0), max(x0));
    fprintf('初始点 x0 和: %f\n', sum(x0));
      % 验证初始点是否满足约束条件
    [g0, h0] = fun2(x0, stock_risk, index_risk, style_weight_upper, style_weight_lower, industry_weight_upper, industry_weight_lower, V, index_initial_weight, te_value, style_len);
    assignin('base', 'g0', g0);
    assignin('base', 'h0', h0);
    fprintf('初始点约束条件 g0 中的 NaN: %d, Inf: %d\n', sum(isnan(g0(:))), sum(isinf(g0(:))));
    fprintf('初始点约束条件 g0 范围: [%f, %f]\n', min(g0), max(g0));
    % 设置非线性约束
    nonlcon = @(x)fun2(x, stock_risk, index_risk, style_weight_upper, style_weight_lower, industry_weight_upper, industry_weight_lower, V, index_initial_weight, te_value, style_len);
    
    % 设置线性约束
    A = []; 
    b = [];
    Aeq = ones(1, stock_number);
    beq = ones(1, 1);
    fprintf('线性等式约束维度: Aeq(%dx%d), beq(%dx%d)\n', size(Aeq, 1), size(Aeq, 2), size(beq, 1), size(beq, 2));
    
    % 设置优化选项
    options = optimoptions('fmincon', ...
        'Display', 'iter-detailed', ...  % 更详细的输出
        'Algorithm', config.optimization_params.Algorithm, ...
        'MaxFunctionEvaluations', config.optimization_params.MaxFunctionEvaluations, ...
        'UseParallel', config.optimization_params.UseParallel, ...
        'CheckGradients', true, ...  % 检查梯度
        'FiniteDifferenceType', 'central', ...  % 使用中心差分
        'FiniteDifferenceStepSize', 1e-6);  % 设置差分步长
    fprintf('优化选项设置完成: 算法=%s, 最大函数评估次数=%d, 并行计算=%s\n', ...
        config.optimization_params.Algorithm, ...
        config.optimization_params.MaxFunctionEvaluations, ...
        string(config.optimization_params.UseParallel));
    
    % 求解优化问题
    fprintf('开始求解优化问题...\n');
    [x, fval, exitflag, output] = fmincon(f, x0, A, b, Aeq, beq, lb, ub, nonlcon, options);
    
    % 检查是否有更好的可行解
    if isfield(output, 'bestfeasible') && ~isempty(output.bestfeasible)
        fprintf('找到更好的可行解，使用该解\n');
        x = output.bestfeasible.x;
        fval = output.bestfeasible.fval;
    end
    
    % 输出优化结果信息
    fprintf('优化完成，输出标志: %d\n', exitflag);
    fprintf('目标函数值: %f\n', fval);
    
    % 根据退出标志输出详细信息
    switch exitflag
        case 1
            fprintf('优化成功完成：\n');
            fprintf('  找到满足约束条件的局部最小值\n');
        case 2
            fprintf('优化成功完成：\n');
            fprintf('  目标函数在可行域内呈现非递减趋势\n');
            fprintf('  约束条件在可行域内满足要求\n');
        case 0
            fprintf('优化达到最大迭代次数：\n');
            fprintf('  最后一步梯度范数: %e\n', output.firstorderopt);
            fprintf('  约束违反值: %e\n', output.constrviolation);
        case -1
            fprintf('优化被输出函数终止\n');
        case -2
            fprintf('未找到可行解\n');
        otherwise
            fprintf('优化因其他原因终止\n');
    end
    
    fprintf('函数评估次数: %d\n', output.funcCount);
    if isfield(output, 'firstorderopt')
        fprintf('一阶最优性: %e\n', output.firstorderopt);
    end
    if isfield(output, 'constrviolation')
        fprintf('约束违反值: %e\n', output.constrviolation);
    end
    
    % 计算结果
    portfolio_risk = sqrt((x - index_initial_weight)' * V * (x - index_initial_weight) * 252);
    portfolio_barra_risk = barra_stock_risk' * x;
    portfolio_industry_risk = industry_stock_risk' * x;
    final_score = score_stock * x;
    final_weight = x;
    weight_sum = sum(final_weight);
    
    fprintf('投资组合风险: %f\n', portfolio_risk);
    fprintf('最终分数: %f\n', final_score);
    fprintf('权重总和: %f\n', weight_sum);
    fprintf('风格风险维度: %dx%d\n', size(portfolio_barra_risk, 1), size(portfolio_barra_risk, 2));
    fprintf('行业风险维度: %dx%d\n', size(portfolio_industry_risk, 1), size(portfolio_industry_risk, 2));
    
    % 计算风险比率
    industry_risk_ratio = (portfolio_industry_risk - industry_index_risk') ./ abs(industry_index_risk');
    fprintf('行业风险比率维度: %dx%d\n', size(industry_risk_ratio, 1), size(industry_risk_ratio, 2));
    
    % 保存风险信息
    barra_saving_info = [portfolio_barra_risk, barra_index_risk', ...
        (portfolio_barra_risk - barra_index_risk') ./ abs(barra_index_risk'), ...
        repmat(portfolio_risk, size(portfolio_barra_risk)), repmat(final_score, size(portfolio_barra_risk)), repmat(weight_sum, size(portfolio_barra_risk))];
    
    industry_saving_info = [portfolio_industry_risk, industry_index_risk', industry_risk_ratio];
    
    fprintf('风格风险信息维度: %dx%d\n', size(barra_saving_info, 1), size(barra_saving_info, 2));
    fprintf('行业风险信息维度: %dx%d\n', size(industry_saving_info, 1), size(industry_saving_info, 2));
    
    % 保存结果
    weight_path = fullfile(path, config.output_files.weight);
    barra_risk_path = fullfile(path, config.output_files.barra_risk);
    industry_risk_path = fullfile(path, config.output_files.industry_risk);
    
    csvwrite(weight_path, final_weight);
    csvwrite(barra_risk_path, barra_saving_info);
    csvwrite(industry_risk_path, industry_saving_info);
    
    fprintf('结果已保存至:\n');
    fprintf('  权重: %s\n', weight_path);
    fprintf('  风格风险: %s\n', barra_risk_path);
    fprintf('  行业风险: %s\n', industry_risk_path);
    
    % 报告总时间
    fprintf('优化完成，总时间: %.2f秒\n', toc);
    
catch e
    % 捕获并处理错误
    fprintf('错误: %s\n', e.message);
    fprintf('堆栈跟踪:\n');
    disp(e.stack);
    
    % 确保返回值不为空
    if ~exist('final_weight', 'var')
        final_weight = [];
        fprintf('由于错误，返回空的final_weight\n');
    end
    if ~exist('barra_saving_info', 'var')
        barra_saving_info = [];
        fprintf('由于错误，返回空的barra_saving_info\n');
    end
    if ~exist('industry_saving_info', 'var')
        industry_saving_info = [];
        fprintf('由于错误，返回空的industry_saving_info\n');
    end
    
    % 重新抛出错误
    rethrow(e);
end

% 非线性约束函数
function [g, h] = fun2(x, stock_risk, index_risk, style_weight_upper, style_weight_lower, industry_weight_upper, industry_weight_lower, V, index_initial_weight, TE, style_len)
    % 分离风格和行业风险
    barra_stock_risk = stock_risk(:, 1:style_len);
    industry_stock_risk = stock_risk(:, style_len+1:end);
    barra_index_risk = index_risk(:, 1:style_len);
    industry_index_risk = index_risk(:, style_len+1:end);
    
    % 计算投资组合风险暴露
    portfolio_barra_risk = barra_stock_risk' * x;
    portfolio_industry_risk = industry_stock_risk' * x;
    
    % 计算约束条件，确保不会出现复数
    barra_diff = portfolio_barra_risk - barra_index_risk';
    industry_diff = portfolio_industry_risk - industry_index_risk';
    
    % 计算约束条件，使用 real 函数确保结果为实数
    barra_index_abs = abs(barra_index_risk');
    industry_index_abs = abs(industry_index_risk');
    
    g1_upper = barra_diff - style_weight_upper .* barra_index_abs;
    g1_lower = -barra_diff + style_weight_lower .* barra_index_abs;
    g2_upper = industry_diff - industry_weight_upper .* industry_index_abs;
    g2_lower = -industry_diff + industry_weight_lower .* industry_index_abs;
    
    % 计算跟踪误差
    diff_weight = x - index_initial_weight;
    V_diff = V * diff_weight;
    quad_term = diff_weight' * V_diff;
    annual_term = quad_term * 252;
    tracking_error = sqrt(real(annual_term));
    g3 = tracking_error - TE;
    
    % 组合所有约束条件
    g = [g1_upper; g1_lower; g2_upper; g2_lower; g3];
    h = [];
end

end
