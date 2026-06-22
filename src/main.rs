use quorum::ping_pong::PingPongNode;
use quorum::{NodeId, Simulator};

fn main() {
    let node_count = 2;
    let mut sim = Simulator::new(42, 100, 1);

    for id in 0..node_count {
        let node_id = NodeId(id);
        sim.spawn(node_id, Box::new(PingPongNode::new(node_id, node_count)));
    }

    for _ in 0..5 {
        sim.step();
    }

    // S3 proves message causality after the first tick-driven ping. Continuous
    // ticking resumes in later acceptance slices.
    sim.tick_interval = 0;
    sim.run(40);

    for event in sim.state.events {
        println!("{}", serde_json::to_string(&event).unwrap());
    }
}
