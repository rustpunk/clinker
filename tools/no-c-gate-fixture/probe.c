/* The smallest translation unit that still requires a working C compiler. Its
   contents do not matter; that it must be compiled at all is the whole point. */
int clinker_no_c_gate_probe(void) {
    return 0;
}
