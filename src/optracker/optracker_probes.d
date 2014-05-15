provider ceph_optracker {
  probe op_event(
    string class_id,
    string inst_id,
    string event);
  probe res_event(
    string res_type_id,
    string res_class_id,
    string res_inst_id,
    string op_class_id,
    string op_inst_id,
    string event,
    string state);
}
