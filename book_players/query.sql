-- the main idea here is to create a feature store, where each row will contain 
-- the players stats from the last 30 days
WITH
    tb_lobby as
    (SELECT
        *
    FROM
        tb_lobby_stats_player
    WHERE
        dtCreatedAt < date('2022-02-01') AND
        dtCreatedAt > date('2022-02-01', '-30 day')     
    )
,

    tb_stats as (

    SELECT
        idPlayer,
        count(distinct idLobbyGame) as qtyLobby,
        count(distinct date(dtCreatedAt)) as qtyDays,
        count(distinct case when qtRoundsPlayed < 16 then idLobbyGame end) as qtyLobbyLess16,
        round(1.0 * count(distinct idLobbyGame) / count(distinct date(dtCreatedAt)), 4 )as avgLobbyDay,
        avg(qtKill) as avgqtyKill,
        avg(qtAssist) as avgqtyAssist,
        avg(qtDeath) as avgqtyDeath,
        1.0*avg(qtKill/qtDeath) as avgKD,
        1.0*sum(qtKill)/sum(qtDeath) as KDgeral,
        1.0*avg((qtKill + qtAssist )/ qtDeath) as avgKDA,
        1.0*(sum(qtKill + qtAssist)) / sum(qtDeath) as KDAgeral,
        1.0*avg((qtKill + qtAssist )/ qtRoundsPlayed) as avgKAround,
        1.0*(sum(qtKill + qtAssist)) / sum(qtRoundsPlayed) as KAroundgeral,
        avg(qtHs) as avgqtyHs,
        avg(1.0*qtHs /qtKill) as avgHSrate,
        1.0*sum(qtHs) / sum(qtKill) as txHSGeral,
        avg(qtBombeDefuse) as avgqtyBombeDefuse,
        avg(qtBombePlant) as avgqtyBombePlant,
        avg(qtTk) as avgqtyTk,
        avg(qtTkAssist) as avgqtyTkAssist,
        avg(qt1Kill) as avgqty1Kill,
        avg(qt2Kill) as avgqty2Kill,
        avg(qt3Kill) as avgqty3Kill,
        avg(qt4Kill) as avgqty4Kill,
        sum(qt4Kill) as sumqty4Kill,
        avg(qt5Kill) as avgqty5Kill,
        sum(qt5Kill) as sumqty5Kill,
        avg(qtPlusKill) as avgqtyPlusKill,
        avg(qtFirstKill) as avgqtyFirstKill,
        avg(vlDamage) as avgvlDamage,
        avg(vlDamage / qtRoundsPlayed) as avgDamageRound,
        1.0*(sum(vlDamage)) / sum(qtRoundsPlayed) as DamageRoundGeral,
        avg(qtHits) as avgqtyHits,
        avg(qtShots) as avgqtyShots,
        avg(qtLastAlive) as avgqtyLastAlive,
        avg(qtClutchWon) as avgqtyClutchWon,
        avg(qtRoundsPlayed) as avgqtyRoundsPlayed,
        avg(vlLevel) as avgvlLevel,
        avg(qtSurvived) as avgqtySurvived,
        avg(qtTrade) as avgqtyTrade,
        avg(qtFlashAssist) as avgqtyFlashAssist,
        avg(qtHitHeadshot) as avgqtyHitHeadshot,
        avg(qtHitChest) as avgqtyHitChest,
        avg(qtHitStomach) as avgqtyHitStomach,
        avg(qtHitLeftArm) as avgqtyHitLeftArm,
        avg(qtHitRightArm) as avgqtyHitRightArm,
        avg(qtHitLeftLeg) as avgqtyHitLeftLeg,
        avg(qtHitRightLeg) as avgqtyHitRightLeg,
        avg(flWinner) as avgflWinner,
        count(distinct case when descMapName = "de_mirage" then idLobbyGame end) as qtyLobbyMirage,
        count(distinct case when descMapName = "de_mirage" and flWinner = 1 then idLobbyGame end) as qtyWinsMirage,
        count(distinct case when descMapName = "de_nuke" then idLobbyGame end) as qtyLobbyNuke,
        count(distinct case when descMapName = "de_nuke" and flWinner = 1 then idLobbyGame end) as qtyWinsNuke,
        count(distinct case when descMapName = "de_inferno" then idLobbyGame end) as qtyLobbyInferno,
        count(distinct case when descMapName = "de_inferno" and flWinner = 1 then idLobbyGame end) as qtyWinsInferno,
        count(distinct case when descMapName = "de_vertigo" then idLobbyGame end) as qtyLobbyVertigo,
        count(distinct case when descMapName = "de_vertigo" and flWinner = 1 then idLobbyGame end) as qtyWinsVertigo,
        count(distinct case when descMapName = "de_ancient" then idLobbyGame end) as qtyLobbyAncient,
        count(distinct case when descMapName = "de_ancient" and flWinner = 1 then idLobbyGame end) as qtyWinsAncient,
        count(distinct case when descMapName = "de_dust2" then idLobbyGame end) as qtyLobbyDust2,
        count(distinct case when descMapName = "de_dust2" and flWinner = 1 then idLobbyGame end) as qtyWinsDust2,
        count(distinct case when descMapName = "de_train" then idLobbyGame end) as qtyLobbyTrain,
        count(distinct case when descMapName = "de_train" and flWinner = 1 then idLobbyGame end) as qtyWinsTrain,
        count(distinct case when descMapName = "de_overpass" then idLobbyGame end) as qtyLobbyOverpass,
        count(distinct case when descMapName = "de_overpass" and flWinner = 1 then idLobbyGame end) as qtyWinsOverpass

    FROM
        tb_lobby

    GROUP BY
        1
    )
,

    tb_current_lvl as (
    SELECT
        idPlayer,
        vlLevel
    FROM 
    (
        SELECT
            idPlayer,
            vlLevel,
            row_number() over(partition by idPlayer order by dtCreatedAt desc) as rn
        FROM
            tb_lobby)
    WHERE
        rn = 1
    )

SELECT
    t1.*,
    t2.vlLevel as vlCurrentLevel
FROM
    tb_stats as t1
left JOIN
    tb_current_lvl as t2
on t1.idPlayer = t2.idPlayer