# ETL Script Cleanup Plan

## Summary of Analysis
After analyzing all 25 ETL scripts in `src/etl`, I've identified significant redundancy and obsolete functionality. This plan outlines specific actions to streamline the ETL pipeline.

## Immediate Removal Candidates (8 scripts - 32% reduction)

### Debug/One-time Scripts (Remove completely)
1. **`dbnomics_debug.py`** - Temporary connectivity testing
2. **`dbnomics_probe.py`** - Duplicate connectivity testing
3. **`debug_fundamentals.py`** - One-time EODHD API debugging
4. **`find_candidate_tags.py`** - One-time taxonomy exploration
5. **`finra_capability_probe.py`** - One-time FINRA API testing
6. **`prove_aggregation.py`** - One-time data aggregation proof
7. **`sec_taxonomy_explorer.py`** - One-time taxonomy analysis
8. **`verify_data_parity.py`** - One-time data parity verification

**Action**: Delete these files immediately as they served temporary purposes and have no ongoing value.

## Consolidation Candidates

### 1. Price Pipeline Consolidation
**Current**: `price_pipeline.py` (YFinance) vs `eodhd_price_pipeline.py` (EODHD)
**Issue**: Both fetch market price data with overlapping functionality
**Action**: Merge `price_pipeline.py` into `eodhd_price_pipeline.py` and delete the former

### 2. CIK-Ticker Mapping Consolidation
**Current**: `cik_ticker_mapper.py` vs `build_historical_cik_map.py` + `universe_pipeline.py`
**Issue**: Three scripts handling CIK-ticker mapping with significant overlap
**Action**: Merge `cik_ticker_mapper.py` functionality into `build_historical_cik_map.py` and update `universe_pipeline.py` to use the consolidated mapper

### 3. Market Data Collection Consolidation
**Current**: `multi_source_market_collector.py` vs `comprehensive_market_pipeline.py`
**Issue**: Both collect market data from multiple sources
**Action**: Merge `multi_source_market_collector.py` into `comprehensive_market_pipeline.py`

### 4. Monitoring Consolidation
**Current**: `assess_silver_quality.py` vs `market_data_monitor.py`
**Issue**: Both monitor data quality with overlapping metrics
**Action**: Consolidate quality assessment functionality into `market_data_monitor.py`

## Review Candidates (Potential Removal)

### Specialized Scripts with Limited Value
1. **`fetch_missing_fundamentals.py`** - Specialized script that may overlap with main pipelines
2. **`global_macro_pipeline.py`** - Limited functionality that could be merged with `fred_pipeline.py`

**Action**: Review usage patterns and consider merging into core pipelines or removing if unused.

## Implementation Plan

### Phase 1: Immediate Cleanup (Week 1)
- [ ] Delete 8 debug/one-time scripts
- [ ] Update documentation to remove references
- [ ] Verify no dependencies exist on removed scripts

### Phase 2: Consolidation (Week 2-3)
- [ ] Merge price pipeline functionality
- [ ] Consolidate CIK-ticker mapping
- [ ] Integrate market data collection
- [ ] Unify monitoring functionality

### Phase 3: Review and Optimization (Week 4)
- [ ] Assess specialized scripts for removal/merging
- [ ] Optimize remaining core pipelines
- [ ] Update documentation and examples

## Expected Benefits
- **32% immediate reduction** in script count (8/25 files)
- **Improved maintainability** through consolidation
- **Clearer architecture** with focused core pipelines
- **Reduced technical debt** from obsolete code

## Risk Assessment
- **Low risk**: Debug scripts have no production dependencies
- **Medium risk**: Consolidation requires careful testing
- **High value**: Significant reduction in code complexity

## Recommendation
Proceed with Phase 1 (immediate removal) first, then evaluate consolidation impact before proceeding to Phase 2.