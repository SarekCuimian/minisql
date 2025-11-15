package top.guoziyang.mydb.client;

import top.guoziyang.mydb.backend.utils.format.ExecResult;
import top.guoziyang.mydb.backend.utils.format.ExecResultCodec;
import top.guoziyang.mydb.transport.Package;
import top.guoziyang.mydb.transport.Packager;

public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public ExecResult execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return ExecResultCodec.decode(resPkg.getData());
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
