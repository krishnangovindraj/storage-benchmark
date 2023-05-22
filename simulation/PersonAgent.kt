/*
 * Copyright (C) 2022 Vaticle
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
package com.vaticle.typedb.benchmarks.storage

import com.vaticle.typedb.client.api.TypeDBSession
import com.vaticle.typedb.benchmarks.storage.common.Context
import com.vaticle.typedb.client.api.TypeDBTransaction
import com.vaticle.typedb.simulation.Agent
import com.vaticle.typedb.simulation.common.seed.RandomSource
import com.vaticle.typedb.simulation.typedb.TypeDBClient

import com.vaticle.typeql.lang.TypeQL

public class PersonAgent(client: TypeDBClient, context: Context) :
    Agent<Context.DBPartition, TypeDBSession, Context>(client, context) {

    override val agentClass = PersonAgent::class.java
    override val partitions = context.partitions

    private fun nameFrom(partitionId: Int, id: Int): String {
        return "name" + partitionId + ":" + id
    }

    fun createPerson(
        session: TypeDBSession,
        dbPartition: Context.DBPartition,
        randomSource: RandomSource
    ): List<Agent.Report> {
        session.transaction(TypeDBTransaction.Type.WRITE).use { tx ->
            for (i in 1..context.model.personPerBatch) {
                val name : String = nameFrom(dbPartition.partitionId, dbPartition.idCtr.addAndGet(1))
                tx.query().insert(TypeQL.insert(
                    TypeQL.`var`("p").isa("person").has("name", name)))
            }
            tx.commit()
        }
        return listOf()
    }

    fun createFriendship(
        session: TypeDBSession,
        dbPartition: Context.DBPartition,
        randomSource: RandomSource
    ): List<Agent.Report> {
        session.transaction(TypeDBTransaction.Type.WRITE).use { tx ->
            for (i in 1..context.model.friendshipPerBatch) {
                val first : String = nameFrom(dbPartition.partitionId, randomSource.nextInt(dbPartition.idCtr.get()))
                val second : String = nameFrom(dbPartition.partitionId, randomSource.nextInt(dbPartition.idCtr.get()))
                tx.query().insert(TypeQL.match(
                    TypeQL.`var`("p1").isa("person").has("name", first),
                    TypeQL.`var`("p2").isa("person").has("name", second),
                    ).insert(
                        TypeQL.rel("person","p1").rel("person","p1").isa("friendship")
                    ))
            }
            tx.commit()
        }
        return listOf()
    }

    override val actionHandlers = mapOf(
        "createPerson" to ::createPerson,
        "createFriendship" to ::createFriendship
    )
}
