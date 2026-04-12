# \# E-Commerce Data Platform

# 

# An end-to-end enterprise data engineering platform that ingests, 

# processes, transforms, and serves e-commerce data using a modern 

# open-source stack.

# 

# \## Architecture

# 

# Real-time order events flow through Kafka into Spark Structured 

# Streaming, landing in Delta Lake format on AWS S3. Simultaneously, 

# operational data from PostgreSQL is batch-extracted to S3. Airflow 

# orchestrates both pipelines. dbt transforms raw data in Snowflake 

# into clean mart models. Metabase serves the business dashboard.

# 

# \## Tech Stack

# 

# | Layer | Technology |

# |-------|-----------|

# | Event streaming | Apache Kafka |

# | Stream processing | Spark Structured Streaming |

# | Batch processing | PySpark |

# | Orchestration | Apache Airflow |

# | Data lake | AWS S3 + Delta Lake |

# | Operational DB | PostgreSQL |

# | Data warehouse | Snowflake |

# | Transformation | dbt Core |

# | Data quality | Great Expectations + dbt tests |

# | Infrastructure | Terraform |

# | CI/CD | GitHub Actions |

# | Visualization | Metabase |

# 

# \## Setup

# 

# Instructions coming as each phase is completed.

# 

# \## Status

# 

# Currently in Phase 1 — Environment Setup

