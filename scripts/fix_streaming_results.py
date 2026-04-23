with open("data_quality/validate_streaming.py", "r", encoding="utf-8") as f:
    src = f.read()

old = """    if not result.success:
        logger.error("FAIL %s validation FAILED", topic)
        for vr in result.run_results.values():
            for er in vr["validation_result"]["results"]:
                if not er["success"]:
                    logger.error("  FAIL %s  result=%s",
                        er["expectation_config"]["type"], er.get("result", {}))"""

new = """    if not result.success:
        logger.error("FAIL %s validation FAILED", topic)
        for vr in result.run_results.values():
            vr_results = getattr(vr, "results", None) or vr.get("results", [])
            for er in vr_results:
                try:
                    success = er["success"] if isinstance(er, dict) else er.success
                    if not success:
                        cfg = er["expectation_config"]["type"] if isinstance(er, dict) else er.expectation_config.type
                        res = er.get("result", {}) if isinstance(er, dict) else getattr(er, "result", {})
                        logger.error("  FAIL %s  result=%s", cfg, res)
                except Exception:
                    pass"""

if old in src:
    src = src.replace(old, new)
    with open("data_quality/validate_streaming.py", "w", encoding="utf-8", newline="\n") as f:
        f.write(src)
    print("Fixed validate_streaming.py")
else:
    print("Pattern not found - printing current error block for inspection")
    idx = src.find("if not result.success")
    print(repr(src[idx:idx+400]))