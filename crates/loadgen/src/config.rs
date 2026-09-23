use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "loadgen")]
pub struct LoadgenConfig {
    #[arg(long, env = "GATEWAY_URL", default_value = "http://localhost:8080/v1/events")]
    pub gateway_url: String,

    #[arg(long, default_value_t = 200)]
    pub events_per_sec: u64,

    #[arg(long, default_value_t = 60)]
    pub duration_secs: u64,

    #[arg(long, default_value_t = 5_000)]
    pub num_users: u64,

    #[arg(long, default_value_t = 1.2)]
    pub zipf_exponent: f64,

    #[arg(long, default_value_t = 0.01)]
    pub duplicate_rate: f64,

    #[arg(long, default_value_t = 0.005)]
    pub late_rate: f64,

    #[arg(long, default_value_t = 42)]
    pub seed: u64,

    #[arg(long, default_value_t = 100)]
    pub batch_size: usize,

    #[arg(long, default_value = "truth.json")]
    pub truth_file: String,
}
