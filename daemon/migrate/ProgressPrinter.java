/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.daemon.migrate;

import java.util.Timer;
import java.util.TimerTask;

public class ProgressPrinter implements MigrationClient.ProgressListener, AutoCloseable {

    private final String[] ANIM = new String[] {
            "-",
            "\\",
            "|",
            "/"
    };

    private int lines = 0;
    private StringBuilder builder = new StringBuilder();

    private final String type;
    private final Timer timer = new Timer();

    private boolean started = false;
    private String status = "starting";
    private long current = 0;
    private long total = 0;
    private boolean animate = true;

    private int anim;

    public ProgressPrinter(String type, int animRate) {
        this.type = type;

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                step();
            }
        };
        timer.scheduleAtFixedRate(task, 0, animRate);
    }

    private synchronized void step() {
        builder.append(String.format("$x isa %s,\n    has status \"%s\"", type, status));

        if (started) {
            String percent;
            if (total > 0) {
                percent = String.format("%.1f", (double)current / (double)total * 100.0);
            } else {
                percent = "?";
            }
            builder.append(String.format(",\n    has progress (%s%%),\n    has count (%,d / %,d)",
                    percent, current, total));
        }

        builder.append(";");
        if (animate) {
            anim = (anim + 1) % ANIM.length;
            builder.append(" ").append(ANIM[anim]);
        }

        String output = builder.toString();
        System.out.println((lines > 0 ? "\033[" + lines + "F\033[J" : "") + output);

        lines = output.split("\n").length;
        builder = new StringBuilder();
    }

    @Override
    public synchronized void close() throws Exception {
        step();
        timer.cancel();
    }

    @Override
    public void onProgress(long current, long total) {
        started = true;
        status = "in progress";
        animate = true;
        this.current = current;
        this.total = total;
    }

    @Override
    public void onCompletion(long total) {
        status = "completed";
        animate = false;
        this.current = total;
        this.total = total;
    }
}
