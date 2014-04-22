package com.airhacks.enhydrator.transform;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 Adam Bien
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.airhacks.enhydrator.in.Entry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 *
 * @author airhacks.com
 */
public class Expression {

    private final ScriptEngineManager manager;
    private final ScriptEngine engine;
    private Consumer<String> expressionListener;

    public Expression() {
        this(l -> {
        });
    }

    public Expression(Consumer<String> expressionListener) {
        this.expressionListener = expressionListener;
        this.manager = new ScriptEngineManager();
        this.engine = manager.getEngineByName("nashorn");
    }

    public List<Entry> execute(List<Entry> columns, Entry entry, String expression) {
        Bindings bindings = this.engine.createBindings();
        bindings.put("columns", columns);
        bindings.put("current", entry);
        try {
            this.expressionListener.accept("Executing: " + expression);
            Object result = this.engine.eval(expression, bindings);
            this.expressionListener.accept("Got result: " + result);
            if (!(result instanceof List)) {
                return entry.asList();
            } else {
                return (List<Entry>) result;
            }
        } catch (ScriptException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }
}